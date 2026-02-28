import json
import math
import os
import re
from datetime import datetime

import pandas as pd
import pydeck as pdk
import streamlit as st
import streamlit.components.v1 as components
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

try:
    import folium
    from folium.plugins import PolyLineTextPath
except Exception:
    folium = None
    PolyLineTextPath = None

try:
    import osmnx as ox
except Exception:
    ox = None

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:
    st_autorefresh = None

st.set_page_config(layout="wide")
st.title("🚦 Chennai Guindy Junction Traffic Dashboard")

TOPIC = "traffic-decisions"
BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
USE_OSM_GEOMETRY = os.getenv("USE_OSM_GEOMETRY", "1") == "1"

SEVERITY_COLOR = {
    "LOW": "#22c55e",
    "MEDIUM": "#f59e0b",
    "HIGH": "#ef4444",
}

REROUTE_COLOR = "#8b5cf6"
NEW_ROUTE_COLOR = "#2563eb"
REMOVED_SECTION_COLOR = "#b91c1c"
NEUTRAL_ROUTE_COLOR = "#6b7280"
CHENNAI_NORTH = 13.014
CHENNAI_SOUTH = 13.008
CHENNAI_EAST = 80.226
CHENNAI_WEST = 80.218
PREVIOUS_ZOOM_LEVEL = 13
FOLIUM_ZOOM_START = 13
SYNTHETIC_DATASET_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data",
    "synthetic_junction_chennai.json",
)

MANUAL_ROAD_ANCHORS = {
    "Anna Salai": (13.010795167202032, 80.21680275421907),
    "Sardar Patel Road": (13.011792402849073, 80.22231197635944),
    "GST Road": (13.004029819696987, 80.20108071628023),
    "Inner Ring Road": (13.01565750395905, 80.20508877115829),
}

MANUAL_ROAD_PATH_OVERRIDES = {
    "gst rd": [[
        [80.20009522450407, 13.002107660686795],
        [80.20070419869045, 13.00295155070478],
        [80.20285576106863, 13.006257279781057],
        [80.20363217800337, 13.007436507086908],
        [80.20278186962734, 13.00801886688933],
        [80.20480983761996, 13.007690003921208],
    ]],
    "anna salai": [[
        [80.20787048140261, 13.008031354461018],
        [80.20966087953093, 13.008327024428471],
        [80.21228578522663, 13.00925838251943],
        [80.2180240898333, 13.01121202641615],
        [80.21987312587852, 13.012097718538115],
    ]],
    "sardar patel rd": [[
        [80.2224155811407, 13.011808388516169],
        [80.2237911839522, 13.011267598349232],
        [80.22592012101751, 13.010332016695921],
        [80.22885899782416, 13.009136514102352],
        [80.23439709356565, 13.008020361783057],
    ]],
    "inner ring rd": [[
        [80.20639666490945, 13.021442789162808],
        [80.20593734782773, 13.018782599623407],
        [80.20504423124396, 13.015774313436909],
        [80.204789061092, 13.01497308110038],
        [80.20467422578041, 13.014643088892234],
        [80.20401658491703, 13.01275698740642],
        [80.20388783888787, 13.01208797409655],
    ]],
}

LEVEL_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
LOAD_SMOOTHING_ALPHA = 0.80
HIGH_PROMOTION_STREAK = 1
DECISION_STALE_SECONDS = 25
REACTIVE_METRIC_ALPHA = 0.92
IMPACT_SESSION_ALPHA = 0.60
LOAD_LEVEL_MEDIUM_THRESHOLD = 2.35
LOAD_LEVEL_HIGH_THRESHOLD = 2.85


def fetch_recent_messages(max_messages=500, timeout_ms=1000):
    st.session_state.pop("kafka_error", None)
    brokers = [b.strip() for b in BROKER.split(",") if b.strip()]
    if not brokers:
        brokers = ["127.0.0.1:9092"]

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=brokers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_ms,
        )
    except NoBrokersAvailable:
        st.session_state["kafka_error"] = "NoBrokersAvailable"
        return []
    except Exception as exc:
        st.session_state["kafka_error"] = f"{type(exc).__name__}: {exc}"
        return []

    partitions = consumer.partitions_for_topic(TOPIC)
    if not partitions:
        consumer.close()
        return []

    topic_partitions = [TopicPartition(TOPIC, p) for p in sorted(partitions)]
    consumer.assign(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)

    per_partition = max(30, max_messages // max(1, len(topic_partitions)))

    for tp in topic_partitions:
        end = end_offsets.get(tp, 0)
        start = max(0, end - per_partition)
        consumer.seek(tp, start)

    records = []

    while True:
        polled = consumer.poll(timeout_ms=timeout_ms)
        if not polled:
            break
        for _, messages in polled.items():
            for message in messages:
                records.append(message.value)

    consumer.close()
    return records


def parse_time(value):
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.min


def latest_window_records(records, stale_seconds=DECISION_STALE_SECONDS):
    if not records:
        return []

    with_window = [r for r in records if r.get("window_end")]
    if not with_window:
        return []

    latest_time = max(parse_time(r.get("window_end", "")) for r in with_window)
    if latest_time == datetime.min:
        return []

    age_seconds = (datetime.utcnow() - latest_time).total_seconds()
    if age_seconds > float(stale_seconds):
        return []

    latest_key = latest_time.isoformat()
    return [r for r in with_window if r.get("window_end") == latest_key]


def update_session_traffic_state(all_decision_records, metrics_records):
    state = st.session_state.setdefault(
        "traffic_session_state",
        {
            "seen_decision_keys": set(),
            "seen_metrics_keys": set(),
            "windows_seen": set(),
            "total_events_processed": 0,
            "total_high_roads": 0,
            "total_unique_roads": 0,
            "throughput_sum": 0.0,
            "processing_throughput_sum": 0.0,
            "latency_sum": 0.0,
            "latency_p95_sum": 0.0,
            "cpu_sum": 0.0,
            "memory_sum": 0.0,
            "stability_sum": 0.0,
            "window_drop_total": 0,
            "estimated_missed_windows_total": 0,
            "metrics_samples": 0,
            "load_sum": 0.0,
            "load_samples": 0,
            "street_stats": {},
            "reactive_load": 0.0,
            "reactive_high_roads": 0.0,
            "reactive_latency": 0.0,
            "reactive_throughput": 0.0,
            "reactive_reroute_coverage": 0.0,
            "reactive_windows": 0,
            "reroute_coverage_sum": 0.0,
            "reroute_coverage_samples": 0,
        },
    )

    for record in all_decision_records:
        key = (
            record.get("window_end", ""),
            record.get("street", ""),
            int(record.get("probe_id", 0)),
            int(record.get("subtask", 0)),
        )
        if key in state["seen_decision_keys"]:
            continue

        state["seen_decision_keys"].add(key)

        street = str(record.get("street", "")).strip()
        if not street:
            continue

        level = str(record.get("level", "LOW")).upper()
        if level not in {"LOW", "MEDIUM", "HIGH"}:
            level = "LOW"

        load_value = float(record.get("load", 0.0))
        state["load_sum"] += load_value
        state["load_samples"] += 1

        stats = state["street_stats"].setdefault(
            street,
            {
                "load_sum": 0.0,
                "count": 0,
                "last_level": "LOW",
            },
        )
        stats["load_sum"] += load_value
        stats["count"] += 1
        stats["last_level"] = level

    for record in metrics_records:
        key = (
            record.get("window_end", ""),
            int(record.get("subtask", 0)),
        )
        if key in state["seen_metrics_keys"]:
            continue

        state["seen_metrics_keys"].add(key)

        window_end = record.get("window_end")
        if window_end:
            state["windows_seen"].add(window_end)

        state["total_events_processed"] += int(record.get("events_processed", 0))
        state["total_high_roads"] += int(record.get("high_roads", 0))
        state["total_unique_roads"] += int(record.get("unique_roads", 0))
        state["throughput_sum"] += float(record.get("throughput_eps", 0.0))
        state["processing_throughput_sum"] += float(record.get("processing_throughput_eps", 0.0))
        state["latency_sum"] += float(record.get("processing_latency_sec", 0.0))
        state["latency_p95_sum"] += float(record.get("processing_latency_sec", 0.0))
        state["cpu_sum"] += float(record.get("cpu_util_pct", 0.0))
        state["memory_sum"] += float(record.get("memory_mb", 0.0))
        state["stability_sum"] += float(record.get("stability_ratio", 0.0))
        state["window_drop_total"] += int(record.get("window_drop_flag", 0))
        state["estimated_missed_windows_total"] += int(record.get("estimated_missed_windows", 0))
        state["metrics_samples"] += 1

    latest_decision_window = latest_window_records(all_decision_records, stale_seconds=10**9)
    if latest_decision_window:
        latest_load = sum(float(r.get("load", 0.0)) for r in latest_decision_window) / max(1, len(latest_decision_window))

        latest_window_streets = latest_per_street(latest_decision_window)
        latest_high_roads = sum(
            1 for r in latest_window_streets if str(r.get("level", "LOW")).upper() == "HIGH"
        )
        latest_rerouted_high_roads = sum(
            1
            for r in latest_window_streets
            if str(r.get("level", "LOW")).upper() == "HIGH" and r.get("reroute_options")
        )
        latest_reroute_coverage = latest_rerouted_high_roads / max(1, latest_high_roads)

        state["reroute_coverage_sum"] += float(latest_reroute_coverage)
        state["reroute_coverage_samples"] += 1
    else:
        latest_load = None
        latest_high_roads = None
        latest_reroute_coverage = None

    latest_metrics = aggregate_latest_metrics(metrics_records)
    latest_latency = float(latest_metrics.get("avg_latency", 0.0)) if latest_metrics else None
    latest_throughput = float(latest_metrics.get("throughput_eps", 0.0)) if latest_metrics else None

    alpha = float(REACTIVE_METRIC_ALPHA)
    if latest_load is not None:
        if int(state.get("reactive_windows", 0)) <= 0:
            state["reactive_load"] = float(latest_load)
        else:
            state["reactive_load"] = (alpha * float(latest_load)) + ((1.0 - alpha) * float(state.get("reactive_load", latest_load)))

    if latest_high_roads is not None:
        if int(state.get("reactive_windows", 0)) <= 0:
            state["reactive_high_roads"] = float(latest_high_roads)
        else:
            state["reactive_high_roads"] = (alpha * float(latest_high_roads)) + ((1.0 - alpha) * float(state.get("reactive_high_roads", latest_high_roads)))

    if latest_latency is not None:
        if int(state.get("reactive_windows", 0)) <= 0:
            state["reactive_latency"] = float(latest_latency)
        else:
            state["reactive_latency"] = (alpha * float(latest_latency)) + ((1.0 - alpha) * float(state.get("reactive_latency", latest_latency)))

    if latest_throughput is not None:
        if int(state.get("reactive_windows", 0)) <= 0:
            state["reactive_throughput"] = float(latest_throughput)
        else:
            state["reactive_throughput"] = (alpha * float(latest_throughput)) + ((1.0 - alpha) * float(state.get("reactive_throughput", latest_throughput)))

    if latest_reroute_coverage is not None:
        if int(state.get("reactive_windows", 0)) <= 0:
            state["reactive_reroute_coverage"] = float(latest_reroute_coverage)
        else:
            state["reactive_reroute_coverage"] = (alpha * float(latest_reroute_coverage)) + (
                (1.0 - alpha) * float(state.get("reactive_reroute_coverage", latest_reroute_coverage))
            )

    if any(value is not None for value in [latest_load, latest_high_roads, latest_latency, latest_throughput, latest_reroute_coverage]):
        state["reactive_windows"] = int(state.get("reactive_windows", 0)) + 1

    return state


def build_overall_street_rank(state, limit=8):
    ranked = []
    for street_name, stats in state.get("street_stats", {}).items():
        count = int(stats.get("count", 0))
        if count <= 0:
            continue
        avg_load = float(stats.get("load_sum", 0.0)) / count
        ranked.append(
            {
                "street": street_name,
                "load": round(avg_load, 3),
                "level": str(stats.get("last_level", "LOW")).upper(),
                "reroute_options": [],
            }
        )

    ranked.sort(key=lambda x: float(x.get("load", 0.0)), reverse=True)
    return ranked[:limit]


def latest_per_street(records):
    by_street = {}
    for item in records:
        street = item.get("street")
        if not street:
            continue

        existing = by_street.get(street)
        current_time = parse_time(item.get("window_end", ""))
        previous_time = parse_time(existing.get("window_end", "")) if existing else datetime.min

        current_load = float(item.get("load", 0.0))
        previous_load = float(existing.get("load", 0.0)) if existing else -1.0

        if existing is None or current_time > previous_time or (
            current_time == previous_time and current_load >= previous_load
        ):
            by_street[street] = item
    return list(by_street.values())


def filter_records_to_chennai_junction(records):
    filtered = []
    for item in records:
        try:
            lat = float(item.get("lat"))
            lon = float(item.get("lon"))
        except Exception:
            continue

        if CHENNAI_SOUTH <= lat <= CHENNAI_NORTH and CHENNAI_WEST <= lon <= CHENNAI_EAST:
            filtered.append(item)

    return filtered


def aggregate_latest_metrics(metrics_records):
    if not metrics_records:
        return None

    grouped = {}
    for record in metrics_records:
        window_end = record.get("window_end")
        subtask = record.get("subtask", 0)
        if not window_end:
            continue
        grouped.setdefault(window_end, {})[subtask] = record

    if not grouped:
        return None

    latest_window = max(grouped.keys(), key=parse_time)
    per_subtask = list(grouped[latest_window].values())

    events_processed = sum(int(m.get("events_processed", 0)) for m in per_subtask)
    unique_roads = sum(int(m.get("unique_roads", 0)) for m in per_subtask)
    high_roads = sum(int(m.get("high_roads", 0)) for m in per_subtask)
    throughput_eps = sum(float(m.get("throughput_eps", 0.0)) for m in per_subtask)
    processing_throughput_eps = sum(float(m.get("processing_throughput_eps", 0.0)) for m in per_subtask)

    latencies = [float(m.get("processing_latency_sec", 0.0)) for m in per_subtask]
    avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
    if latencies:
        ordered_latencies = sorted(latencies)
        p95_idx = max(0, min(len(ordered_latencies) - 1, int(math.ceil(0.95 * len(ordered_latencies)) - 1)))
        p95_latency = float(ordered_latencies[p95_idx])
    else:
        p95_latency = 0.0

    cpu_values = [float(m.get("cpu_util_pct", 0.0)) for m in per_subtask]
    avg_cpu_util_pct = (sum(cpu_values) / len(cpu_values)) if cpu_values else 0.0

    memory_values = [float(m.get("memory_mb", 0.0)) for m in per_subtask]
    avg_memory_mb = (sum(memory_values) / len(memory_values)) if memory_values else 0.0

    stability_values = [float(m.get("stability_ratio", 0.0)) for m in per_subtask]
    avg_stability_ratio = (sum(stability_values) / len(stability_values)) if stability_values else 0.0

    window_drop_count = sum(int(m.get("window_drop_flag", 0)) for m in per_subtask)
    estimated_missed_windows = sum(int(m.get("estimated_missed_windows", 0)) for m in per_subtask)

    event_counts = [int(m.get("events_processed", 0)) for m in per_subtask]
    active_subtasks = len(event_counts)
    if event_counts and active_subtasks > 0:
        max_events = max(event_counts)
        mean_events = sum(event_counts) / active_subtasks
        variance_events = sum((count - mean_events) ** 2 for count in event_counts) / active_subtasks
        load_imbalance_cv = math.sqrt(variance_events) / max(1e-9, mean_events)
        speedup_estimate = events_processed / max(1, max_events)
        parallel_efficiency = speedup_estimate / max(1, active_subtasks)
    else:
        load_imbalance_cv = 0.0
        speedup_estimate = 0.0
        parallel_efficiency = 0.0

    return {
        "window_end": latest_window,
        "events_processed": events_processed,
        "unique_roads": unique_roads,
        "high_roads": high_roads,
        "throughput_eps": throughput_eps,
        "processing_throughput_eps": processing_throughput_eps,
        "avg_latency": avg_latency,
        "p95_latency": p95_latency,
        "avg_cpu_util_pct": avg_cpu_util_pct,
        "avg_memory_mb": avg_memory_mb,
        "avg_stability_ratio": avg_stability_ratio,
        "window_drop_count": window_drop_count,
        "estimated_missed_windows": estimated_missed_windows,
        "active_subtasks": active_subtasks,
        "speedup_estimate": speedup_estimate,
        "parallel_efficiency": parallel_efficiency,
        "load_imbalance_cv": load_imbalance_cv,
    }


def color_rgb(level):
    if level == "HIGH":
        return [239, 68, 68]
    if level == "MEDIUM":
        return [245, 158, 11]
    return [34, 197, 94]


def path_center(path):
    if not path:
        return [0.0, 0.0]
    lon = sum(p[0] for p in path) / len(path)
    lat = sum(p[1] for p in path) / len(path)
    return [lon, lat]


def nearest_path_to_anchor(paths, anchor_lon, anchor_lat):
    if not paths:
        return []

    scored = []
    for path in paths:
        center_lon, center_lat = path_center(path)
        distance = ((center_lat - anchor_lat) ** 2 + (center_lon - anchor_lon) ** 2) ** 0.5
        scored.append((distance, path))

    scored.sort(key=lambda x: x[0])
    return [scored[0][1]] if scored else []


def anchor_aligned_segment(paths, anchor_lon, anchor_lat, default_span=0.00045):
    nearest = nearest_path_to_anchor(paths, anchor_lon, anchor_lat)
    if nearest:
        path = nearest[0]
        if len(path) >= 2:
            best_idx = 0
            best_dist = 1e9
            for idx, point in enumerate(path):
                lon, lat = float(point[0]), float(point[1])
                dist = ((lat - anchor_lat) ** 2 + (lon - anchor_lon) ** 2) ** 0.5
                if dist < best_dist:
                    best_dist = dist
                    best_idx = idx

            left_idx = max(0, best_idx - 1)
            right_idx = min(len(path) - 1, best_idx + 1)

            if left_idx != right_idx:
                left = [float(path[left_idx][0]), float(path[left_idx][1])]
                right = [float(path[right_idx][0]), float(path[right_idx][1])]
                return [[left, [float(anchor_lon), float(anchor_lat)], right]]

    return [[
        [float(anchor_lon - default_span), float(anchor_lat)],
        [float(anchor_lon + default_span), float(anchor_lat)],
    ]]


def force_paths_through_anchor(paths, anchor_lon, anchor_lat):
    if not paths:
        return []

    nearest = nearest_path_to_anchor(paths, anchor_lon=anchor_lon, anchor_lat=anchor_lat)
    if not nearest:
        return []

    path = nearest[0]
    if len(path) < 2:
        return []

    best_idx = 0
    best_dist = 1e9
    for idx, point in enumerate(path):
        lon, lat = float(point[0]), float(point[1])
        dist = ((lat - anchor_lat) ** 2 + (lon - anchor_lon) ** 2) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best_idx = idx

    anchored_path = [
        [float(p[0]), float(p[1])] for p in path
    ]
    anchor_point = [float(anchor_lon), float(anchor_lat)]

    if best_idx == 0:
        if point_distance(anchor_point, anchored_path[0]) > 1e-9:
            anchored_path = [anchor_point] + anchored_path
    elif best_idx == len(anchored_path) - 1:
        if point_distance(anchor_point, anchored_path[-1]) > 1e-9:
            anchored_path = anchored_path + [anchor_point]
    else:
        left = anchored_path[:best_idx + 1]
        right = anchored_path[best_idx + 1:]
        if point_distance(anchor_point, left[-1]) > 1e-9:
            left.append(anchor_point)
        anchored_path = left + right

    return [anchored_path]


def edge_path_from_graph(graph, u, v, edge_key=None):
    if graph is None:
        return []

    edge_bundle = graph.get_edge_data(u, v, default={})
    if not edge_bundle:
        return []

    edge_data = None
    if edge_key is not None and edge_key in edge_bundle:
        edge_data = edge_bundle.get(edge_key)
    if edge_data is None:
        edge_data = min(edge_bundle.values(), key=lambda entry: float(entry.get("length", 1e12)))

    geom = edge_data.get("geometry") if isinstance(edge_data, dict) else None
    if geom is not None and hasattr(geom, "coords"):
        return [[float(x), float(y)] for x, y in geom.coords]

    try:
        return [
            [float(graph.nodes[u]["x"]), float(graph.nodes[u]["y"])],
            [float(graph.nodes[v]["x"]), float(graph.nodes[v]["y"])],
        ]
    except Exception:
        return []


def anchor_segment_from_graph(graph, anchor_lon, anchor_lat, window_points=2):
    if ox is None or graph is None:
        return []

    try:
        nearest = ox.distance.nearest_edges(graph, X=float(anchor_lon), Y=float(anchor_lat))
    except Exception:
        return []

    if not nearest:
        return []

    if isinstance(nearest, (tuple, list)) and len(nearest) >= 2:
        u = nearest[0]
        v = nearest[1]
        edge_key = nearest[2] if len(nearest) > 2 else None
    else:
        return []

    path = edge_path_from_graph(graph, u, v, edge_key=edge_key)
    if len(path) < 2:
        return []

    nearest_idx = 0
    best_dist = 1e9
    for idx, point in enumerate(path):
        lon, lat = float(point[0]), float(point[1])
        dist = ((lat - anchor_lat) ** 2 + (lon - anchor_lon) ** 2) ** 0.5
        if dist < best_dist:
            best_dist = dist
            nearest_idx = idx

    left_idx = max(0, nearest_idx - max(1, int(window_points)))
    right_idx = min(len(path) - 1, nearest_idx + max(1, int(window_points)))
    segment = path[left_idx:right_idx + 1]
    if len(segment) < 2:
        return []

    return [segment]


def local_segment_from_graph(graph, ref_lon, ref_lat, window_points=6):
    if ox is None or graph is None:
        return []

    try:
        nearest = ox.distance.nearest_edges(graph, X=float(ref_lon), Y=float(ref_lat))
    except Exception:
        return []

    if not nearest:
        return []

    if isinstance(nearest, (tuple, list)) and len(nearest) >= 2:
        u = nearest[0]
        v = nearest[1]
        edge_key = nearest[2] if len(nearest) > 2 else None
    else:
        return []

    path = edge_path_from_graph(graph, u, v, edge_key=edge_key)
    if len(path) < 2:
        return []

    nearest_idx = 0
    best_dist = 1e12
    for idx, point in enumerate(path):
        lon, lat = float(point[0]), float(point[1])
        dist = haversine_meters(float(ref_lat), float(ref_lon), lat, lon)
        if dist < best_dist:
            best_dist = dist
            nearest_idx = idx

    span = max(2, int(window_points))
    left_idx = max(0, nearest_idx - span)
    right_idx = min(len(path) - 1, nearest_idx + span)
    segment = [[float(p[0]), float(p[1])] for p in path[left_idx:right_idx + 1]]
    if len(segment) < 2:
        return []

    return [segment]


def point_distance(a, b):
    if not a or not b:
        return 1e9
    dx = float(a[0]) - float(b[0])
    dy = float(a[1]) - float(b[1])
    return (dx * dx + dy * dy) ** 0.5


def stitch_path_segments(paths, max_join_distance=0.0009):
    clean = [segment for segment in paths if segment and len(segment) >= 2]
    if len(clean) <= 1:
        return clean

    # Start from the longest segment, then greedily connect nearest endpoints.
    clean.sort(key=len, reverse=True)
    merged = clean[0][:]
    remaining = clean[1:]

    while remaining:
        best_index = None
        best_reverse = False
        best_distance = 1e9

        end_point = merged[-1]
        for idx, candidate in enumerate(remaining):
            d_start = point_distance(end_point, candidate[0])
            d_end = point_distance(end_point, candidate[-1])

            if d_start < best_distance:
                best_distance = d_start
                best_index = idx
                best_reverse = False
            if d_end < best_distance:
                best_distance = d_end
                best_index = idx
                best_reverse = True

        if best_index is None:
            break

        segment = remaining.pop(best_index)
        if best_reverse:
            segment = list(reversed(segment))

        if best_distance <= max_join_distance:
            if merged[-1] != segment[0]:
                merged.append(segment[0])
            merged.extend(segment[1:])
        else:
            # Too far to connect; keep as separate painted segment.
            return [merged, segment] + remaining

    return [merged]


def nearest_paths(paths, ref_lon, ref_lat, max_paths=3, max_degree_distance=0.035):
    if not paths:
        return []

    scored = []
    for path in paths:
        center_lon, center_lat = path_center(path)
        distance = ((center_lat - ref_lat) ** 2 + (center_lon - ref_lon) ** 2) ** 0.5
        scored.append((distance, path))

    scored.sort(key=lambda x: x[0])
    filtered = [path for distance, path in scored if distance <= max_degree_distance]

    if not filtered:
        filtered = [path for _, path in scored[:1]]

    return filtered[:max_paths]


def trim_path_around_reference(path, ref_lon, ref_lat, extent_meters=650.0):
    if not path or len(path) < 2:
        return path

    best_idx = 0
    best_dist = 1e12
    for idx, point in enumerate(path):
        lon, lat = float(point[0]), float(point[1])
        dist = haversine_meters(float(ref_lat), float(ref_lon), lat, lon)
        if dist < best_dist:
            best_dist = dist
            best_idx = idx

    left_idx = best_idx
    covered = 0.0
    while left_idx > 0 and covered < float(extent_meters):
        a = path[left_idx]
        b = path[left_idx - 1]
        covered += haversine_meters(float(a[1]), float(a[0]), float(b[1]), float(b[0]))
        left_idx -= 1

    right_idx = best_idx
    covered = 0.0
    while right_idx < len(path) - 1 and covered < float(extent_meters):
        a = path[right_idx]
        b = path[right_idx + 1]
        covered += haversine_meters(float(a[1]), float(a[0]), float(b[1]), float(b[0]))
        right_idx += 1

    segment = [[float(p[0]), float(p[1])] for p in path[left_idx:right_idx + 1]]
    return segment if len(segment) >= 2 else path


def localize_paths_to_decision(paths, ref_lon, ref_lat, extent_meters=650.0):
    if not paths:
        return []

    nearby = nearest_paths(
        paths,
        ref_lon=float(ref_lon),
        ref_lat=float(ref_lat),
        max_paths=1,
        max_degree_distance=0.06,
    )
    if not nearby:
        return []

    localized = []
    for path in nearby:
        localized.append(
            trim_path_around_reference(
                path,
                ref_lon=float(ref_lon),
                ref_lat=float(ref_lat),
                extent_meters=float(extent_meters),
            )
        )

    return [p for p in localized if p and len(p) >= 2]


def path_heading(path):
    if not path or len(path) < 2:
        return None
    start_lon, start_lat = path[0]
    end_lon, end_lat = path[-1]
    dx = end_lon - start_lon
    dy = end_lat - start_lat
    if abs(dx) < 1e-10 and abs(dy) < 1e-10:
        return None
    angle = math.degrees(math.atan2(dy, dx))
    return (angle + 360.0) % 360.0


def heading_diff(a, b):
    if a is None or b is None:
        return 180.0
    diff = abs(a - b) % 360.0
    return min(diff, 360.0 - diff)


def movement_heading(history_points):
    if not history_points or len(history_points) < 2:
        return None
    start_lat, start_lon = history_points[0]
    end_lat, end_lon = history_points[-1]
    dx = end_lon - start_lon
    dy = end_lat - start_lat
    if abs(dx) < 1e-10 and abs(dy) < 1e-10:
        return None
    angle = math.degrees(math.atan2(dy, dx))
    return (angle + 360.0) % 360.0


def pick_directional_paths(paths, ref_lon, ref_lat, movement_angle, max_paths=2, max_degree_distance=0.03):
    if not paths:
        return []

    scored = []
    for path in paths:
        center_lon, center_lat = path_center(path)
        distance = ((center_lat - ref_lat) ** 2 + (center_lon - ref_lon) ** 2) ** 0.5
        seg_angle = path_heading(path)
        direction_penalty = heading_diff(movement_angle, seg_angle) / 180.0
        score = distance + (0.01 * direction_penalty)
        scored.append((score, distance, path))

    scored.sort(key=lambda x: x[0])
    nearby = [path for _, distance, path in scored if distance <= max_degree_distance]
    if not nearby:
        nearby = [path for _, _, path in scored[:1]]
    return nearby[:max_paths]


def geo_distance_km(lat1, lon1, lat2, lon2):
    radius = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    value = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return radius * (2 * math.atan2(math.sqrt(value), math.sqrt(1 - value)))


def haversine_meters(lat1, lon1, lat2, lon2):
    radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    value = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(value), math.sqrt(1 - value))


def collect_viewport_points(path_rows, connector_rows=None, pin_rows=None):
    points = []

    for row in (path_rows or []):
        for pt in row.get("path", []):
            if len(pt) == 2:
                points.append([float(pt[1]), float(pt[0])])  # [lat, lon]

    for row in (connector_rows or []):
        for pt in row.get("path", []):
            if len(pt) == 2:
                points.append([float(pt[1]), float(pt[0])])  # [lat, lon]

    for pin in (pin_rows or []):
        if "lat" in pin and "lon" in pin:
            points.append([float(pin["lat"]), float(pin["lon"])])

    return points


def estimate_zoom_from_bounds(points):
    if not points:
        return 12.6

    min_lat = min(p[0] for p in points)
    max_lat = max(p[0] for p in points)
    min_lon = min(p[1] for p in points)
    max_lon = max(p[1] for p in points)

    span = max(max_lat - min_lat, max_lon - min_lon)
    if span <= 0.002:
        return 15.2
    if span <= 0.004:
        return 14.6
    if span <= 0.008:
        return 14.0
    if span <= 0.015:
        return 13.3
    if span <= 0.03:
        return 12.6
    if span <= 0.06:
        return 11.9
    return 11.2


def route_nodes_to_path(graph, route_nodes):
    if graph is None or not route_nodes or len(route_nodes) < 2:
        return []

    merged = []
    for index in range(len(route_nodes) - 1):
        start_node = route_nodes[index]
        end_node = route_nodes[index + 1]
        edge_bundle = graph.get_edge_data(start_node, end_node, default={})
        if not edge_bundle:
            continue

        edge_data = min(edge_bundle.values(), key=lambda entry: float(entry.get("length", 1e12)))
        geom = edge_data.get("geometry")
        if geom is not None and hasattr(geom, "coords"):
            coords = [[float(x), float(y)] for x, y in geom.coords]
        else:
            coords = [
                [float(graph.nodes[start_node]["x"]), float(graph.nodes[start_node]["y"])],
                [float(graph.nodes[end_node]["x"]), float(graph.nodes[end_node]["y"])],
            ]

        if not merged:
            merged.extend(coords)
        elif merged[-1] == coords[0]:
            merged.extend(coords[1:])
        else:
            merged.extend(coords)

    return merged if len(merged) >= 2 else []


def meters_between_lonlat(a, b):
    if not a or not b:
        return 1e12
    return haversine_meters(float(a[1]), float(a[0]), float(b[1]), float(b[0]))


def orient_path_toward_points(path, start_lon, start_lat, end_lon, end_lat):
    if not path or len(path) < 2:
        return []

    start_ref = [float(start_lon), float(start_lat)]
    end_ref = [float(end_lon), float(end_lat)]

    forward_score = meters_between_lonlat(path[0], start_ref) + meters_between_lonlat(path[-1], end_ref)
    reverse_score = meters_between_lonlat(path[-1], start_ref) + meters_between_lonlat(path[0], end_ref)

    if reverse_score + 1e-6 < forward_score:
        return list(reversed(path))
    return path


def shortest_route_nodes_any_direction(graph, src_node, dst_node):
    if ox is None or graph is None:
        return None

    try:
        forward = ox.shortest_path(graph, src_node, dst_node, weight="length")
    except Exception:
        forward = None

    if forward and len(forward) >= 2:
        return forward

    try:
        reverse = ox.shortest_path(graph, dst_node, src_node, weight="length")
    except Exception:
        reverse = None

    if reverse and len(reverse) >= 2:
        return list(reversed(reverse))

    return None


def build_connector_path(graph, src_lon, src_lat, dst_lon, dst_lat):
    if ox is None or graph is None:
        return []

    try:
        src_node = ox.distance.nearest_nodes(graph, X=float(src_lon), Y=float(src_lat))
        dst_node = ox.distance.nearest_nodes(graph, X=float(dst_lon), Y=float(dst_lat))
    except Exception:
        return []

    if src_node == dst_node:
        return []

    route_nodes = shortest_route_nodes_any_direction(graph, src_node, dst_node)
    if not route_nodes or len(route_nodes) < 2:
        return []

    connector = route_nodes_to_path(graph, route_nodes)
    return connector if len(connector) >= 2 else []


def map_match_history_to_osm_paths(graph, history_points, max_points=12, max_hop_meters=2500):
    if ox is None or graph is None or not history_points or len(history_points) < 2:
        return []

    snapped_nodes = []
    for lat, lon in history_points[-max_points:]:
        try:
            node_id = ox.distance.nearest_nodes(graph, X=float(lon), Y=float(lat))
        except Exception:
            continue
        if not snapped_nodes or snapped_nodes[-1] != node_id:
            snapped_nodes.append(node_id)

    if len(snapped_nodes) < 2:
        return []

    mapped_path = []
    for idx in range(len(snapped_nodes) - 1):
        src = snapped_nodes[idx]
        dst = snapped_nodes[idx + 1]
        if src == dst:
            continue

        src_lat = float(graph.nodes[src]["y"])
        src_lon = float(graph.nodes[src]["x"])
        dst_lat = float(graph.nodes[dst]["y"])
        dst_lon = float(graph.nodes[dst]["x"])
        if haversine_meters(src_lat, src_lon, dst_lat, dst_lon) > max_hop_meters:
            continue

        try:
            route_nodes = ox.shortest_path(graph, src, dst, weight="length")
        except Exception:
            route_nodes = None

        if not route_nodes or len(route_nodes) < 2:
            continue

        route_path = route_nodes_to_path(graph, route_nodes)
        if len(route_path) < 2:
            continue

        if not mapped_path:
            mapped_path.extend(route_path)
        elif mapped_path[-1] == route_path[0]:
            mapped_path.extend(route_path[1:])
        else:
            mapped_path.extend(route_path)

    return [mapped_path] if len(mapped_path) >= 2 else []


def map_match_template_path_to_osm(graph, template_path, max_hop_meters=15000):
    if ox is None or graph is None or not template_path or len(template_path) < 2:
        return []

    cleaned_points = []
    for lon, lat in template_path:
        point = [float(lon), float(lat)]
        if not cleaned_points:
            cleaned_points.append(point)
            continue
        if meters_between_lonlat(cleaned_points[-1], point) >= 2.0:
            cleaned_points.append(point)

    if len(cleaned_points) < 2:
        return []

    snapped = []
    for lon, lat in cleaned_points:
        try:
            node_id = ox.distance.nearest_nodes(graph, X=float(lon), Y=float(lat))
        except Exception:
            continue
        if not snapped or snapped[-1]["node"] != node_id:
            snapped.append({"node": node_id, "lon": float(lon), "lat": float(lat)})

    if len(snapped) < 2:
        return []

    mapped_path = []
    for idx in range(len(snapped) - 1):
        src = snapped[idx]
        dst = snapped[idx + 1]
        src_node = src["node"]
        dst_node = dst["node"]

        if src_node == dst_node:
            continue

        src_lat = float(graph.nodes[src_node]["y"])
        src_lon = float(graph.nodes[src_node]["x"])
        dst_lat = float(graph.nodes[dst_node]["y"])
        dst_lon = float(graph.nodes[dst_node]["x"])
        if haversine_meters(src_lat, src_lon, dst_lat, dst_lon) > max_hop_meters:
            continue

        route_nodes = shortest_route_nodes_any_direction(graph, src_node, dst_node)

        if not route_nodes or len(route_nodes) < 2:
            continue

        route_path = route_nodes_to_path(graph, route_nodes)
        if len(route_path) < 2:
            continue

        route_path = orient_path_toward_points(
            route_path,
            start_lon=src["lon"],
            start_lat=src["lat"],
            end_lon=dst["lon"],
            end_lat=dst["lat"],
        )

        if not mapped_path:
            mapped_path.extend(route_path)
        else:
            gap_m = meters_between_lonlat(mapped_path[-1], route_path[0])
            if gap_m > 35.0:
                connector = build_connector_path(
                    graph,
                    src_lon=mapped_path[-1][0],
                    src_lat=mapped_path[-1][1],
                    dst_lon=route_path[0][0],
                    dst_lat=route_path[0][1],
                )
                if len(connector) >= 2:
                    if meters_between_lonlat(mapped_path[-1], connector[0]) > meters_between_lonlat(mapped_path[-1], connector[-1]):
                        connector = list(reversed(connector))
                    mapped_path.extend(connector[1:])

            if meters_between_lonlat(mapped_path[-1], route_path[0]) <= 35.0:
                mapped_path.extend(route_path[1:])
            else:
                mapped_path.extend(route_path)

    return [mapped_path] if len(mapped_path) >= 2 else []


def normalize_street_name(name: str) -> str:
    if not name:
        return ""
    value = str(name).lower().strip()
    value = re.sub(r"[^a-z0-9\s]", " ", value)

    replacements = {
        "street": "st",
        "avenue": "ave",
        "road": "rd",
        "boulevard": "blvd",
        "drive": "dr",
        "lane": "ln",
        "place": "pl",
        "parkway": "pkwy",
        "highway": "hwy",
        "terrace": "ter",
        "square": "sq",
    }
    for source, target in replacements.items():
        value = re.sub(rf"\b{source}\b", target, value)

    value = re.sub(r"\s+", " ", value).strip()
    return value


def tokenize_street_name(name: str):
    normalized = normalize_street_name(name)
    if not normalized:
        return []
    return [token for token in normalized.split(" ") if token]


def resolve_street_paths(street_name, street_paths):
    normalized = normalize_street_name(street_name)
    if not normalized:
        return []

    exact = street_paths.get(normalized, [])
    if exact:
        return exact

    query_tokens = set(tokenize_street_name(street_name))
    if not query_tokens:
        return []

    scored_matches = []
    for candidate_name, candidate_paths in street_paths.items():
        candidate_tokens = set(candidate_name.split(" "))
        overlap = len(query_tokens & candidate_tokens)
        if overlap == 0:
            continue

        coverage = overlap / max(1, len(query_tokens))
        if coverage < 0.5:
            continue

        bonus = 0.0
        if normalized in candidate_name or candidate_name in normalized:
            bonus = 0.25

        score = coverage + bonus
        scored_matches.append((score, candidate_paths))

    if not scored_matches:
        return []

    scored_matches.sort(key=lambda item: item[0], reverse=True)
    best_score = scored_matches[0][0]
    merged = []
    for score, paths in scored_matches:
        if score + 1e-9 < best_score:
            break
        merged.extend(paths)

    return merged


def extract_osm_names(raw_name):
    if raw_name is None:
        return []
    if isinstance(raw_name, list):
        return [str(x) for x in raw_name if x]
    return [str(raw_name)]


@st.cache_resource(show_spinner=False)
def get_osm_street_index(north, south, east, west):
    if ox is None:
        return {}, {}, None

    try:
        # OSMnx v2 expects bbox in (left, bottom, right, top) = (west, south, east, north)
        graph = ox.graph_from_bbox(
            (west, south, east, north),
            network_type="drive",
            simplify=True,
        )
        edges = ox.graph_to_gdfs(graph, nodes=False, edges=True, fill_edge_geometry=True)
    except Exception:
        return {}, {}, None

    street_paths = {}
    street_centers = {}

    for row in edges.itertuples():
        names = extract_osm_names(getattr(row, "name", None))
        geometry = getattr(row, "geometry", None)
        if geometry is None:
            continue

        paths = []
        if geometry.geom_type == "LineString":
            paths.append([[float(x), float(y)] for x, y in geometry.coords])
        elif geometry.geom_type == "MultiLineString":
            for part in geometry.geoms:
                paths.append([[float(x), float(y)] for x, y in part.coords])

        for raw_name in names:
            normalized = normalize_street_name(raw_name)
            if not normalized:
                continue

            for path in paths:
                if len(path) < 2:
                    continue
                street_paths.setdefault(normalized, []).append(path)
                mid = path[len(path) // 2]
                street_centers.setdefault(normalized, []).append(mid)

    averaged_centers = {}
    for key, values in street_centers.items():
        if not values:
            continue
        lon = sum(v[0] for v in values) / len(values)
        lat = sum(v[1] for v in values) / len(values)
        averaged_centers[key] = [lon, lat]

    return street_paths, averaged_centers, graph


@st.cache_resource(show_spinner=False)
def get_synthetic_street_paths():
    if not os.path.exists(SYNTHETIC_DATASET_PATH):
        return {}

    try:
        with open(SYNTHETIC_DATASET_PATH, "r", encoding="utf-8") as file:
            raw = json.load(file)
    except Exception:
        return {}

    root = raw.get("junction_data", raw)
    roads = root.get("roads", [])
    mapped = {}
    for road in roads:
        name = normalize_street_name(road.get("name", ""))
        coords = road.get("coords", [])
        if not name or len(coords) < 2:
            continue
        path = [[float(lon), float(lat)] for lat, lon in coords]
        mapped[name] = [path]
    return mapped


@st.cache_resource(show_spinner=False)
def get_synthetic_street_catalog():
    if not os.path.exists(SYNTHETIC_DATASET_PATH):
        return []

    try:
        with open(SYNTHETIC_DATASET_PATH, "r", encoding="utf-8") as file:
            raw = json.load(file)
    except Exception:
        return []

    root = raw.get("junction_data", raw)

    catalog = []
    for road in root.get("roads", []):
        road_name = str(road.get("name", "")).strip()
        coords = road.get("coords", [])
        normalized = normalize_street_name(road_name)
        if not road_name or not normalized or len(coords) < 2:
            continue

        latitudes = [float(lat) for lat, _ in coords]
        longitudes = [float(lon) for _, lon in coords]

        catalog.append({
            "name": road_name,
            "normalized": normalized,
            "center_lat": sum(latitudes) / len(latitudes),
            "center_lon": sum(longitudes) / len(longitudes),
            "coords": [[float(lat), float(lon)] for lat, lon in coords],
        })

    return catalog


def build_street_segments(decision_records, max_points_per_street=24, max_jump=0.012):
    histories = {}

    for item in decision_records:
        street = item.get("street")
        if not street:
            continue
        try:
            lat = float(item["lat"])
            lon = float(item["lon"])
        except Exception:
            continue

        histories.setdefault(street, []).append(
            (parse_time(item.get("window_end", "")), lat, lon)
        )

    segmented = {}
    for street, points in histories.items():
        points.sort(key=lambda x: x[0])
        tail = points[-max_points_per_street:]

        street_segments = []
        current_segment = []

        for _, lat, lon in tail:
            if not current_segment:
                current_segment.append([lon, lat])
                continue

            prev_lon, prev_lat = current_segment[-1]
            if abs(lat - prev_lat) <= max_jump and abs(lon - prev_lon) <= max_jump:
                current_segment.append([lon, lat])
            else:
                if len(current_segment) >= 2:
                    street_segments.append(current_segment)
                current_segment = [[lon, lat]]

        if len(current_segment) >= 2:
            street_segments.append(current_segment)

        segmented[street] = street_segments

    return segmented


def build_street_histories(decision_records, max_points_per_street=20):
    histories = {}

    for item in decision_records:
        street = item.get("street")
        if not street:
            continue
        try:
            lat = float(item["lat"])
            lon = float(item["lon"])
        except Exception:
            continue

        histories.setdefault(street, []).append(
            (parse_time(item.get("window_end", "")), lat, lon)
        )

    cleaned = {}
    for street, points in histories.items():
        points.sort(key=lambda x: x[0])
        tail = points[-max_points_per_street:]

        coords = []
        for _, lat, lon in tail:
            if not coords:
                coords.append([lat, lon])
                continue
            prev_lat, prev_lon = coords[-1]
            if abs(lat - prev_lat) > 1e-5 or abs(lon - prev_lon) > 1e-5:
                coords.append([lat, lon])

        cleaned[street] = coords

    return cleaned


def build_guidance_path(graph, src_lat, src_lon, dst_lat, dst_lon):
    if ox is None or graph is None:
        return [[float(src_lon), float(src_lat)], [float(dst_lon), float(dst_lat)]]

    try:
        src_node = ox.distance.nearest_nodes(graph, X=float(src_lon), Y=float(src_lat))
        dst_node = ox.distance.nearest_nodes(graph, X=float(dst_lon), Y=float(dst_lat))
        route_nodes = ox.shortest_path(graph, src_node, dst_node, weight="length")
    except Exception:
        route_nodes = None

    if not route_nodes or len(route_nodes) < 2:
        return [[float(src_lon), float(src_lat)], [float(dst_lon), float(dst_lat)]]

    route_path = route_nodes_to_path(graph, route_nodes)
    if len(route_path) < 2:
        return [[float(src_lon), float(src_lat)], [float(dst_lon), float(dst_lat)]]
    return route_path


def stabilize_street_states(streets, refresh_seconds):
    if not streets:
        return streets

    cache = st.session_state.setdefault("street_state_cache", {})
    now_ts = datetime.utcnow().timestamp()
    stale_seconds = max(90.0, float(refresh_seconds) * 20.0)

    def level_from_load(load_value):
        if float(load_value) >= float(LOAD_LEVEL_HIGH_THRESHOLD):
            return "HIGH"
        if float(load_value) >= float(LOAD_LEVEL_MEDIUM_THRESHOLD):
            return "MEDIUM"
        return "LOW"

    seen_streets = set()

    for item in streets:
        street_name = str(item.get("street", "")).strip()
        if not street_name:
            continue

        current_load = float(item.get("load", 0.0))
        prev = cache.get(street_name)

        if prev is None:
            smoothed_load = current_load
        else:
            prev_smoothed = float(prev.get("smoothed_load", current_load))
            smoothed_load = ((1.0 - LOAD_SMOOTHING_ALPHA) * prev_smoothed) + (LOAD_SMOOTHING_ALPHA * current_load)

        display_level = level_from_load(smoothed_load)

        item["load"] = smoothed_load
        item["level"] = display_level
        item["model_level"] = display_level
        item["raw_level"] = display_level
        item["raw_load"] = round(current_load, 3)

        cache[street_name] = {
            "smoothed_load": smoothed_load,
            "display_level": display_level,
            "last_seen": now_ts,
        }
        seen_streets.add(street_name)

    stale_keys = [
        key
        for key, value in cache.items()
        if key not in seen_streets and (now_ts - float(value.get("last_seen", now_ts))) > stale_seconds
    ]
    for key in stale_keys:
        cache.pop(key, None)

    return streets


def build_reroute_text(row):
    level = str(row.get("level", "LOW")).upper()

    if level in {"LOW", "MEDIUM"}:
        return "Low congestion road (LOW/MEDIUM); reroute not required"

    reroute_details = row.get("reroute_details", []) or []
    if reroute_details:
        labeled = []
        for item in reroute_details:
            street_name = str(item.get("street", "")).strip()
            level = str(item.get("level", "")).upper()
            if not street_name:
                continue
            if level in {"LOW", "MEDIUM"}:
                labeled.append(f"{street_name} (low congestion)")
            else:
                labeled.append(street_name)
        if labeled:
            return "Reroute: " + ", ".join(labeled)

    reason = str(row.get("reroute_reason", "")).strip()
    if reason and "Low congestion road" not in reason:
        return reason

    return "No reroute: no suitable LOW/MEDIUM alternative right now"


def calculate_optimization_level(streets, session_state):
    visible = streets or []
    if not visible:
        return 0.0, "Low"

    high_streets = [s for s in visible if str(s.get("level", "")).upper() == "HIGH"]
    if high_streets:
        rerouted = sum(1 for s in high_streets if s.get("reroute_options"))
        reroute_success = rerouted / max(1, len(high_streets))
    else:
        reroute_success = 1.0

    reactive_high_roads = float(session_state.get("reactive_high_roads", 0.0))
    avg_high_roads = reactive_high_roads if int(session_state.get("reactive_windows", 0)) > 0 else (
        float(session_state.get("total_high_roads", 0.0)) / max(1, len(session_state.get("windows_seen", set())))
    )
    street_count = max(1, len(session_state.get("street_stats", {})))
    congestion_control = 1.0 - min(1.0, avg_high_roads / street_count)

    if int(session_state.get("reactive_windows", 0)) > 0:
        avg_load = float(session_state.get("reactive_load", 0.0))
    else:
        load_samples = int(session_state.get("load_samples", 0))
        if load_samples > 0:
            avg_load = float(session_state.get("load_sum", 0.0)) / load_samples
        else:
            avg_load = sum(float(s.get("load", 0.0)) for s in visible) / max(1, len(visible))
    load_efficiency = 1.0 - min(1.0, avg_load / 3.0)

    score = (0.45 * reroute_success) + (0.35 * congestion_control) + (0.20 * load_efficiency)
    score_pct = round(score * 100.0, 1)

    if score_pct >= 80:
        return score_pct, "Excellent"
    if score_pct >= 65:
        return score_pct, "High"
    if score_pct >= 45:
        return score_pct, "Medium"
    return score_pct, "Low"


def estimate_no_rerouting_impact(streets, avg_load, avg_speed_kmh, avg_delay):
    active_streets = streets or []
    street_count = max(1, len(active_streets))
    high_total = sum(1 for row in active_streets if str(row.get("level", "LOW")).upper() == "HIGH")
    rerouted_high = sum(
        1
        for row in active_streets
        if str(row.get("level", "LOW")).upper() == "HIGH" and row.get("reroute_options")
    )
    reroute_coverage = rerouted_high / max(1, high_total)

    load_multiplier = 1.0 + (0.06 + (0.16 * reroute_coverage))
    baseline_load = max(0.0, min(4.0, float(avg_load) * load_multiplier))

    baseline_delay = max(float(avg_delay), 1.5 + (baseline_load * 8.5))
    baseline_speed_mph = max(10.0, 68.0 - (baseline_load * 22.0))
    baseline_speed_kmh = baseline_speed_mph * 1.60934

    additional_high = max(1, int(round(rerouted_high * 0.65))) if rerouted_high > 0 else 0
    baseline_high = min(street_count, high_total + additional_high)

    current_clogging_index = min(100.0, (float(avg_load) * 22.0) + (float(high_total) * 4.5))
    baseline_clogging_index = min(100.0, (float(baseline_load) * 22.0) + (float(baseline_high) * 4.5))

    # Operational estimate: emissions increase with delay/load/high-congestion roads.
    current_emission_kgph = max(0.0, (street_count * 12.0) + (float(avg_delay) * 2.4) + (float(avg_load) * 9.0) + (float(high_total) * 3.0))
    baseline_emission_kgph = max(0.0, (street_count * 12.0) + (float(baseline_delay) * 2.4) + (float(baseline_load) * 9.0) + (float(baseline_high) * 3.0))

    return {
        "current_speed_kmh": round(float(avg_speed_kmh), 2),
        "current_delay": round(float(avg_delay), 2),
        "high_total": high_total,
        "rerouted_high": rerouted_high,
        "reroute_coverage_pct": round(reroute_coverage * 100.0, 1),
        "baseline_load": round(baseline_load, 3),
        "baseline_delay": round(float(baseline_delay), 2),
        "baseline_speed_kmh": round(float(baseline_speed_kmh), 2),
        "baseline_high_roads": int(baseline_high),
        "current_clogging_index": round(float(current_clogging_index), 2),
        "baseline_clogging_index": round(float(baseline_clogging_index), 2),
        "current_emission_kgph": round(float(current_emission_kgph), 2),
        "baseline_emission_kgph": round(float(baseline_emission_kgph), 2),
        "speed_drop": round(max(0.0, float(avg_speed_kmh) - float(baseline_speed_kmh)), 2),
        "delay_increase": round(max(0.0, float(baseline_delay) - float(avg_delay)), 2),
        "high_road_increase": int(max(0, int(baseline_high) - int(high_total))),
        "clogging_reduction": round(max(0.0, float(baseline_clogging_index) - float(current_clogging_index)), 2),
        "clogging_reduction_pct": round(
            max(0.0, ((float(baseline_clogging_index) - float(current_clogging_index)) / max(1e-9, float(baseline_clogging_index))) * 100.0),
            1,
        ),
        "co2_reduction_kgph": round(max(0.0, float(baseline_emission_kgph) - float(current_emission_kgph)), 2),
    }


def update_impact_session_state(snapshot, window_key):
    tracked_fields = [
        "current_speed_kmh",
        "current_delay",
        "high_total",
        "reroute_coverage_pct",
        "baseline_speed_kmh",
        "baseline_delay",
        "baseline_high_roads",
        "baseline_load",
        "speed_drop",
        "delay_increase",
        "high_road_increase",
        "current_clogging_index",
        "baseline_clogging_index",
        "clogging_reduction",
        "clogging_reduction_pct",
        "current_emission_kgph",
        "baseline_emission_kgph",
        "co2_reduction_kgph",
    ]

    state = st.session_state.setdefault(
        "impact_session_state",
        {
            "seen_keys": set(),
            "samples": 0,
            "sums": {field: 0.0 for field in tracked_fields},
            "ema": {},
        },
    )

    should_add = False
    effective_key = str(window_key).strip() if window_key else ""

    if effective_key:
        if effective_key not in state["seen_keys"]:
            state["seen_keys"].add(effective_key)
            should_add = True
    elif int(state.get("samples", 0)) <= 0:
        state["seen_keys"].add("session-bootstrap")
        should_add = True

    if should_add:
        alpha = max(0.05, min(0.98, float(IMPACT_SESSION_ALPHA)))
        for field in tracked_fields:
            value = float(snapshot.get(field, 0.0))
            state["sums"][field] += value
            if field not in state["ema"]:
                state["ema"][field] = value
            else:
                state["ema"][field] = (alpha * value) + ((1.0 - alpha) * float(state["ema"].get(field, value)))
        state["samples"] = int(state.get("samples", 0)) + 1

    if state.get("ema"):
        averaged = {field: float(state["ema"].get(field, 0.0)) for field in tracked_fields}
    else:
        samples = max(1, int(state.get("samples", 0)))
        averaged = {
            field: float(state["sums"].get(field, 0.0)) / samples
            for field in tracked_fields
        }
    averaged["samples"] = int(state.get("samples", 0))
    return averaged


def render_dashboard(refresh_seconds, use_osm_geometry):
    records = fetch_recent_messages()
    all_decision_records = [r for r in records if r.get("record_type", "decision") == "decision"]
    all_decision_records = filter_records_to_chennai_junction(all_decision_records)
    decision_records = latest_window_records(all_decision_records)
    metrics_records = [r for r in records if r.get("record_type") == "metrics"]
    session_state = update_session_traffic_state(all_decision_records, metrics_records)

    kafka_error = st.session_state.get("kafka_error")
    if kafka_error:
        st.error(f"Kafka broker unavailable at {BROKER}. Please start/check Kafka and retry.")
        st.info("Tip: Run docker services and confirm broker port mapping before launching dashboard.")

    streets = latest_per_street(decision_records)
    latest_metrics = aggregate_latest_metrics(metrics_records)
    fallback_segments = build_street_segments(decision_records)
    street_histories = build_street_histories(decision_records)

    if not streets and not kafka_error:
        st.warning("No recent decision messages found. Showing session-level metrics; live road layer will update when new windows arrive.")

    for item in streets:
        item["lat"] = float(item["lat"])
        item["lon"] = float(item["lon"])
        item["load"] = float(item["load"])
        item["level"] = str(item["level"]).upper()

    synthetic_street_paths = get_synthetic_street_paths()
    synthetic_street_catalog = get_synthetic_street_catalog()
    synthetic_road_names = {entry["normalized"] for entry in synthetic_street_catalog}

    streets = stabilize_street_states(streets, refresh_seconds)

    all_streets_by_name = {s["street"]: s for s in streets}

    # Keep reroute targets strictly to roads that are currently LOW/MEDIUM.
    for item in streets:
        options = item.get("reroute_options", []) or []
        filtered_options = []
        for road_name in options:
            target_row = all_streets_by_name.get(str(road_name))
            if not target_row:
                continue
            target_level = str(target_row.get("level", "LOW")).upper()
            if target_level in {"LOW", "MEDIUM"}:
                filtered_options.append(str(road_name))
        item["reroute_options"] = filtered_options

        details = item.get("reroute_details", []) or []
        filtered_details = []
        for detail in details:
            road_name = str(detail.get("street", "")).strip()
            target_row = all_streets_by_name.get(road_name)
            if not target_row:
                continue
            target_level = str(target_row.get("level", "LOW")).upper()
            if target_level in {"LOW", "MEDIUM"}:
                filtered_details.append({
                    "street": road_name,
                    "level": target_level,
                })
        item["reroute_details"] = filtered_details

        if str(item.get("level", "LOW")).upper() == "HIGH" and not filtered_options:
            item["reroute_reason"] = "No reroute: no suitable LOW/MEDIUM alternative right now"

    street_paths, street_centers, osm_graph = {}, {}, None
    if use_osm_geometry and ox is not None and streets:
        # Query a tight area around current streets to avoid huge Overpass requests.
        raw_lats = [s["lat"] for s in streets]
        raw_lons = [s["lon"] for s in streets]

        for row in streets:
            anchor = MANUAL_ROAD_ANCHORS.get(str(row.get("street", "")))
            if anchor is None:
                continue
            anchor_lat, anchor_lon = anchor
            raw_lats.append(float(anchor_lat))
            raw_lons.append(float(anchor_lon))

        min_lat, max_lat = min(raw_lats), max(raw_lats)
        min_lon, max_lon = min(raw_lons), max(raw_lons)

        lat_span = max(0.04, min(0.20, (max_lat - min_lat) + 0.03))
        lon_span = max(0.04, min(0.20, (max_lon - min_lon) + 0.03))

        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2

        north = center_lat + lat_span / 2
        south = center_lat - lat_span / 2
        east = center_lon + lon_span / 2
        west = center_lon - lon_span / 2

        street_paths, street_centers, osm_graph = get_osm_street_index(
            round(north, 3),
            round(south, 3),
            round(east, 3),
            round(west, 3),
        )
        if not street_paths:
            st.warning("Road geometry fetch unavailable now; using fallback geometry.")

    if streets:
        avg_lat = sum(s["lat"] for s in streets) / len(streets)
        avg_lon = sum(s["lon"] for s in streets) / len(streets)
    else:
        avg_lat = (CHENNAI_NORTH + CHENNAI_SOUTH) / 2
        avg_lon = (CHENNAI_EAST + CHENNAI_WEST) / 2

    high_street_names = {s["street"] for s in streets if s["level"] == "HIGH"}
    alternative_street_names = set()
    for item in streets:
        if item["level"] == "HIGH":
            alternative_street_names.update(item.get("reroute_options", []))

    streets = sorted(streets, key=lambda x: float(x.get("load", 0.0)), reverse=True)

    path_data = []
    anchor_pin_data = []
    full_fill_for_zoom = False
    full_fill_road_names = synthetic_road_names
    for item in streets:
        street_name = item["street"]
        level = item["level"]
        normalized_name = normalize_street_name(street_name)
        road_full_fill = full_fill_for_zoom and normalized_name in full_fill_road_names
        anchor_latlon = MANUAL_ROAD_ANCHORS.get(street_name)

        if anchor_latlon is not None:
            anchor_lat, anchor_lon = anchor_latlon
            anchor_pin_data.append({
                "street": street_name,
                "lat": float(anchor_lat),
                "lon": float(anchor_lon),
                "level": level,
            })

        if level == "HIGH":
            path_color_rgb = [185, 28, 28]
            color_hex = REMOVED_SECTION_COLOR
            display_level = "HIGH"
            segment_type = "CONGESTED"
            width = 6
        elif level == "MEDIUM":
            path_color_rgb = [245, 158, 11]
            color_hex = "#f59e0b"
            display_level = "MEDIUM"
            segment_type = "NORMAL"
            width = 5
        else:
            path_color_rgb = [34, 197, 94]
            color_hex = "#22c55e"
            display_level = "LOW"
            segment_type = "NORMAL"
            width = 4

        matched_paths = []
        history = street_histories.get(street_name, [])
        move_angle = movement_heading(history)
        map_matched_paths = map_match_history_to_osm_paths(osm_graph, history)
        named_osm_paths = resolve_street_paths(street_name, street_paths)
        synthetic_paths = synthetic_street_paths.get(normalized_name, [])
        manual_override_paths = MANUAL_ROAD_PATH_OVERRIDES.get(normalized_name, [])
        using_manual_override = bool(manual_override_paths)

        manual_override_osm_paths = []
        if using_manual_override and osm_graph is not None:
            for template_path in manual_override_paths:
                manual_override_osm_paths.extend(map_match_template_path_to_osm(osm_graph, template_path))

        if using_manual_override:
            matched_paths = manual_override_osm_paths if manual_override_osm_paths else manual_override_paths

        synthetic_osm_paths = []
        if (not using_manual_override) and synthetic_paths and osm_graph is not None:
            for template_path in synthetic_paths:
                synthetic_osm_paths.extend(map_match_template_path_to_osm(osm_graph, template_path))

        if (not using_manual_override) and osm_graph is not None:
            matched_paths = local_segment_from_graph(
                osm_graph,
                ref_lon=float(item["lon"]),
                ref_lat=float(item["lat"]),
                window_points=7,
            )

        if not matched_paths and synthetic_osm_paths:
            matched_paths = synthetic_osm_paths
        elif not matched_paths and synthetic_paths:
            matched_paths = synthetic_paths
        elif not matched_paths and named_osm_paths:
            matched_paths = pick_directional_paths(
                named_osm_paths,
                ref_lon=item["lon"],
                ref_lat=item["lat"],
                movement_angle=move_angle,
                max_paths=max(1, len(matched_paths)) if road_full_fill else 3,
                max_degree_distance=0.08 if road_full_fill else 0.04,
            )
        elif not matched_paths and map_matched_paths:
            matched_paths = map_matched_paths
        elif not matched_paths:
            matched_paths = fallback_segments.get(street_name, [])
            matched_paths = nearest_paths(
                matched_paths,
                ref_lon=item["lon"],
                ref_lat=item["lat"],
                max_paths=max(1, len(matched_paths)) if road_full_fill else 3,
                max_degree_distance=0.08 if road_full_fill else 0.04,
            )

        if not using_manual_override:
            matched_paths = stitch_path_segments(
                matched_paths,
                max_join_distance=0.0015 if road_full_fill else 0.0009,
            )

            matched_paths = localize_paths_to_decision(
                matched_paths,
                ref_lon=float(item["lon"]),
                ref_lat=float(item["lat"]),
                extent_meters=700.0,
            )

        if not matched_paths and anchor_latlon is not None:
            anchor_lat, anchor_lon = anchor_latlon
            fallback_anchor_segment = anchor_segment_from_graph(
                osm_graph,
                anchor_lon=float(anchor_lon),
                anchor_lat=float(anchor_lat),
            )
            if fallback_anchor_segment:
                matched_paths = fallback_anchor_segment

        for segment in matched_paths:
            path_data.append({
                "street": street_name,
                "level": display_level,
                "load": round(float(item["load"]), 3),
                "point_index": "-",
                "path": segment,
                "color": path_color_rgb,
                "color_hex": color_hex,
                "width": width,
                "segment_type": segment_type,
            })

    streets_by_name = {s["street"]: s for s in streets}
    congestion_messages = []
    reroute_links = []
    reroute_guidance_rows = []

    for source in streets:
        if source["level"] != "HIGH":
            continue
        reroutes = source.get("reroute_options", [])
        if not reroutes:
            continue

        congestion_messages.append(
            f"🚨 {source['street']} is congested (HIGH). Suggested reroute: {', '.join(reroutes)}"
        )

        row = {
            "congested_road": source["street"],
            "congested_load": round(float(source.get("load", 0.0)), 3),
            "reroute_1": "-",
            "reroute_2": "-",
        }

        for idx, target_name in enumerate(reroutes[:2], start=1):
            target = streets_by_name.get(target_name) or all_streets_by_name.get(target_name)
            if not target:
                continue

            distance_km = geo_distance_km(source["lat"], source["lon"], target["lat"], target["lon"])
            row[f"reroute_{idx}"] = f"{target_name} ({distance_km:.2f} km)"

            reroute_links.append({
                "source_street": source["street"],
                "target_street": target_name,
                "source_lat": source["lat"],
                "source_lon": source["lon"],
                "target_lat": target["lat"],
                "target_lon": target["lon"],
                "distance_km": round(distance_km, 3),
                "guidance_path": build_guidance_path(
                    osm_graph,
                    source["lat"],
                    source["lon"],
                    target["lat"],
                    target["lon"],
                ),
            })

        reroute_guidance_rows.append(row)

    if congestion_messages:
        st.error("\n".join(congestion_messages[:6]))

    low_count = sum(1 for s in streets if s["level"] == "LOW")
    med_count = sum(1 for s in streets if s["level"] == "MEDIUM")
    high_count = sum(1 for s in streets if s["level"] == "HIGH")

    ranked = sorted(streets, key=lambda x: float(x.get("load", 0.0)), reverse=True)[:8]
    overall_ranked = build_overall_street_rank(session_state, limit=8)
    optimization_source_rows = ranked if ranked else overall_ranked
    optimization_score, optimization_label = calculate_optimization_level(optimization_source_rows, session_state)

    if int(session_state.get("reactive_windows", 0)) > 0:
        avg_load = float(session_state.get("reactive_load", 0.0))
    elif int(session_state.get("load_samples", 0)) > 0:
        avg_load = float(session_state.get("load_sum", 0.0)) / max(1, int(session_state.get("load_samples", 0)))
    elif streets:
        avg_load = sum(float(s.get("load", 0.0)) for s in streets) / max(1, len(streets))
    else:
        avg_load = 0.0

    session_windows = max(1, len(session_state.get("windows_seen", set())))
    session_samples = max(1, int(session_state.get("metrics_samples", 0)))
    if int(session_state.get("reactive_windows", 0)) > 0:
        session_avg_latency = float(session_state.get("reactive_latency", 0.0))
        session_avg_throughput = float(session_state.get("reactive_throughput", 0.0))
        session_avg_high_roads = float(session_state.get("reactive_high_roads", 0.0))
        session_reroute_coverage = float(session_state.get("reactive_reroute_coverage", 0.0))
    else:
        session_avg_latency = float(session_state.get("latency_sum", 0.0)) / session_samples
        session_avg_throughput = float(session_state.get("throughput_sum", 0.0)) / session_samples
        session_avg_high_roads = float(session_state.get("total_high_roads", 0)) / session_windows
        session_reroute_coverage = float(session_state.get("reroute_coverage_sum", 0.0)) / max(1, int(session_state.get("reroute_coverage_samples", 0)))

    session_avg_processing_throughput = float(session_state.get("processing_throughput_sum", 0.0)) / session_samples
    session_avg_cpu_util_pct = float(session_state.get("cpu_sum", 0.0)) / session_samples
    session_avg_memory_mb = float(session_state.get("memory_sum", 0.0)) / session_samples
    session_avg_stability_ratio = float(session_state.get("stability_sum", 0.0)) / session_samples
    session_window_drops = int(session_state.get("window_drop_total", 0))
    session_estimated_missed_windows = int(session_state.get("estimated_missed_windows_total", 0))

    latest_p95_latency = float(latest_metrics.get("p95_latency", 0.0)) if latest_metrics else session_avg_latency
    latest_processing_throughput = float(latest_metrics.get("processing_throughput_eps", 0.0)) if latest_metrics else session_avg_processing_throughput
    latest_parallel_efficiency = float(latest_metrics.get("parallel_efficiency", 0.0)) if latest_metrics else 0.0
    latest_load_imbalance_cv = float(latest_metrics.get("load_imbalance_cv", 0.0)) if latest_metrics else 0.0
    latest_speedup_estimate = float(latest_metrics.get("speedup_estimate", 0.0)) if latest_metrics else 0.0
    latest_active_subtasks = int(latest_metrics.get("active_subtasks", 0)) if latest_metrics else 0
    latest_avg_cpu_util_pct = float(latest_metrics.get("avg_cpu_util_pct", 0.0)) if latest_metrics else session_avg_cpu_util_pct
    latest_avg_memory_mb = float(latest_metrics.get("avg_memory_mb", 0.0)) if latest_metrics else session_avg_memory_mb
    latest_stability_ratio = float(latest_metrics.get("avg_stability_ratio", 0.0)) if latest_metrics else session_avg_stability_ratio
    latest_window_drop_count = int(latest_metrics.get("window_drop_count", 0)) if latest_metrics else 0
    latest_estimated_missed_windows = int(latest_metrics.get("estimated_missed_windows", 0)) if latest_metrics else 0

    session_reroute_coverage = max(0.0, min(1.0, session_reroute_coverage))
    street_count = max(1, len(session_state.get("street_stats", {})) or len(streets))
    congestion_pressure = max(0.0, min(1.0, float(session_avg_high_roads) / max(1, street_count)))

    congestion_penalty = (float(avg_load) * 14.0) + (congestion_pressure * 18.0)
    latency_penalty = min(12.0, float(session_avg_latency) * 45.0)
    throughput_relief = min(6.0, float(session_avg_throughput) * 0.45)
    reroute_relief = float(session_reroute_coverage) * (4.0 + (10.0 * congestion_pressure))

    avg_speed = max(
        8.0,
        min(
            82.0,
            62.0 - congestion_penalty - latency_penalty + throughput_relief + reroute_relief,
        ),
    )
    avg_delay = max(
        1.0,
        (2.0 + (float(avg_load) * 6.8) + (congestion_pressure * 9.5) + (float(session_avg_latency) * 12.0))
        - (float(session_reroute_coverage) * (1.5 + (5.0 * congestion_pressure))),
    )
    traffic_severity = min(
        5.0,
        max(
            1.0,
            (float(avg_load) * 1.15) + (congestion_pressure * 2.35) + min(1.2, float(session_avg_latency) * 3.0),
        ),
    )
    visibility = max(1.0, 8.0 - (avg_load * 1.1))
    history_rows = st.session_state.setdefault("kpi_trend_history", [])
    history_keys = st.session_state.setdefault("kpi_trend_seen_keys", set())
    history_key = None
    history_label = None
    if latest_metrics and latest_metrics.get("window_end"):
        history_key = str(latest_metrics["window_end"])
        history_label = history_key
    elif int(session_state.get("reactive_windows", 0)) > 0:
        history_key = f"reactive-{int(session_state.get('reactive_windows', 0))}"
        history_label = f"Session {int(session_state.get('reactive_windows', 0))}"

    if history_key and history_key not in history_keys:
        history_keys.add(history_key)
        history_rows.append({
            "Window": history_label,
            "Speed": round(float(avg_speed), 3),
            "Delay": round(float(avg_delay), 3),
        })
        if len(history_rows) > 80:
            trimmed = history_rows[-80:]
            st.session_state["kpi_trend_history"] = trimmed
            st.session_state["kpi_trend_seen_keys"] = {str(row["Window"]) for row in trimmed}

    no_reroute = estimate_no_rerouting_impact(streets, avg_load, avg_speed, avg_delay)
    impact_window_key = history_key if history_key else (latest_metrics.get("window_end") if latest_metrics else "")
    impact_session = update_impact_session_state(no_reroute, impact_window_key)

    latest_window_key = str(latest_metrics.get("window_end", "")) if latest_metrics else ""
    latest_window_subtasks = {
        int(row.get("subtask", 0))
        for row in metrics_records
        if latest_window_key and str(row.get("window_end", "")) == latest_window_key
    }
    all_metric_subtasks = {int(row.get("subtask", 0)) for row in metrics_records}

    st.sidebar.markdown("### Stream Infrastructure")
    st.sidebar.caption("Kafka + Flink runtime view")

    st.sidebar.markdown("#### Kafka")
    if kafka_error:
        st.sidebar.error("Broker: DISCONNECTED")
        st.sidebar.caption(f"Reason: {kafka_error}")
    else:
        st.sidebar.success("Broker: CONNECTED")
    st.sidebar.caption(f"Broker: {BROKER}")

    st.sidebar.markdown("#### Flink")
    if latest_metrics and float(latest_metrics.get("throughput_eps", 0.0)) > 0.0:
        st.sidebar.success("Job State: RUNNING")
    elif metrics_records:
        st.sidebar.warning("Job State: IDLE / LOW TRAFFIC")
    else:
        st.sidebar.warning("Job State: WAITING FOR METRICS")

    st.sidebar.metric("Active Subtasks", str(len(latest_window_subtasks)))
    st.sidebar.metric("Events Processed", str(int(session_state.get("total_events_processed", 0))))
    st.sidebar.metric("Current Throughput", f"{session_avg_throughput:.2f} ev/s")
    st.sidebar.metric("Current Latency", f"{session_avg_latency:.3f} s")
    st.sidebar.metric("CPU (avg)", f"{latest_avg_cpu_util_pct:.1f}%")
    st.sidebar.metric("Memory (avg)", f"{latest_avg_memory_mb:.1f} MB")
    st.sidebar.metric("Stability", f"{(latest_stability_ratio * 100.0):.1f}%")

    st.sidebar.markdown("#### Parallelism")
    st.sidebar.metric("Kafka Partitions", "3")
    st.sidebar.metric("Flink Parallelism (Task Manager)", 3)
    st.sidebar.metric("Speedup (est)", f"{latest_speedup_estimate:.2f}x")
    st.sidebar.metric("Efficiency", f"{(latest_parallel_efficiency * 100.0):.1f}%")
    st.sidebar.metric("Load Imbalance", f"{(latest_load_imbalance_cv * 100.0):.1f}%")
    

    if latest_window_key:
        st.sidebar.caption(f"Latest window: {latest_window_key}")
    else:
        st.sidebar.caption("Latest window: waiting for Flink output")

    st.markdown(
        """
        <style>
        .dashboard-card {
            background: linear-gradient(180deg, #101828 0%, #0b1220 100%);
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 12px 14px;
            color: #e5e7eb;
        }
        .dashboard-card .label {
            color: #9ca3af;
            font-size: 12px;
        }
        .dashboard-card .value {
            color: #f9fafb;
            font-size: 24px;
            font-weight: 700;
            margin-top: 4px;
        }
        div[data-testid="stMetric"] {
            padding: 0.3rem 0.2rem;
        }
        div[data-testid="stMetricLabel"] p {
            font-size: 1.05rem;
            font-weight: 500;
        }
        div[data-testid="stMetricValue"] {
            font-size: 2.05rem;
            font-weight: 500;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<h2 style='margin:0 0 0.35rem 0;'>Key Performance Indicators</h2>", unsafe_allow_html=True)

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Average Speed", f"{avg_speed:.1f} km/h")
    k2.metric("Average Delay", f"{avg_delay:.1f} mins")
    k3.metric("Traffic Severity", f"{traffic_severity:.1f}/5")
    k4.metric("Avg High Roads", f"{session_avg_high_roads:.2f}")
    k5.metric("Throughput", f"{session_avg_throughput:.2f} ev/s")
    k6.metric("Latency", f"{session_avg_latency:.3f} s")

    p1, p2, p3, p4, p5, p6 = st.columns(6)
    p1.metric("CPU Avg", f"{latest_avg_cpu_util_pct:.1f}%")
    p2.metric("Memory Avg", f"{latest_avg_memory_mb:.1f} MB")
    p3.metric("Parallel Eff.", f"{(latest_parallel_efficiency * 100.0):.1f}%")
    p4.metric("Imbalance (CV)", f"{(latest_load_imbalance_cv * 100.0):.1f}%")
    p5.metric("Speedup (est)", f"{latest_speedup_estimate:.2f}x")
    p6.metric("Stability", f"{(latest_stability_ratio * 100.0):.1f}%")

    tab_map, tab_load, tab_impact, tab_raw = st.tabs([
        "Traffic Map",
        "Load Balancing",
        "Impact",
        "Final Resultant Data",
    ])

    with tab_map:
        map_col, decision_col = st.columns([2.2, 1.0])

        with map_col:
            st.caption("Legend: HIGH=Red, MEDIUM=Yellow, LOW=Green")
            if folium is not None:
                traffic_map = folium.Map(
                    location=[avg_lat, avg_lon],
                    zoom_start=FOLIUM_ZOOM_START,
                    tiles="CartoDB Positron",
                    control_scale=True,
                )

                congested_group = folium.FeatureGroup(name="Congested roads", show=True)
                normal_group = folium.FeatureGroup(name="Other roads", show=True)

                for row in path_data:
                    latlon_path = [[pt[1], pt[0]] for pt in row["path"]]
                    tooltip_text = f"{row['street']} | {row['level']} | Load: {row['load']}"
                    polyline = folium.PolyLine(
                        locations=latlon_path,
                        color=row["color_hex"],
                        weight=row["width"],
                        opacity=0.9,
                        smooth_factor=1.0,
                        line_cap="round",
                        line_join="round",
                        tooltip=tooltip_text,
                    )

                    if row["segment_type"] == "CONGESTED":
                        polyline.add_to(congested_group)
                    else:
                        polyline.add_to(normal_group)

                for pin in anchor_pin_data:
                    pin_color = SEVERITY_COLOR.get(str(pin.get("level", "LOW")).upper(), "#22c55e")
                    marker_color = "green"
                    if str(pin.get("level", "LOW")).upper() == "HIGH":
                        marker_color = "red"
                    elif str(pin.get("level", "LOW")).upper() == "MEDIUM":
                        marker_color = "orange"

                    folium.Marker(
                        location=[pin["lat"], pin["lon"]],
                        icon=folium.Icon(color=marker_color, icon="map-marker", prefix="fa"),
                        tooltip=f"{pin['street']} (anchor)",
                    ).add_to(traffic_map)

                congested_group.add_to(traffic_map)
                normal_group.add_to(traffic_map)

                folium.LayerControl(collapsed=False).add_to(traffic_map)

                bounds = collect_viewport_points(
                    path_rows=path_data,
                    connector_rows=[],
                    pin_rows=anchor_pin_data,
                )
                if bounds:
                    min_lat = min(point[0] for point in bounds)
                    max_lat = max(point[0] for point in bounds)
                    min_lon = min(point[1] for point in bounds)
                    max_lon = max(point[1] for point in bounds)

                    lat_span = max(0.004, max_lat - min_lat)
                    lon_span = max(0.004, max_lon - min_lon)

                    south_extra = lat_span * 0.40
                    north_extra = lat_span * 0.22
                    west_extra = lon_span * 0.38
                    east_extra = lon_span * 0.20

                    viewport_bounds = [
                        [min_lat - south_extra, min_lon - west_extra],
                        [max_lat + north_extra, max_lon + east_extra],
                    ]
                    traffic_map.fit_bounds(viewport_bounds, padding=(56, 56))

                legend_html = """
                <div style="
                    position: fixed;
                    top: 20px;
                    left: 20px;
                    z-index: 9999;
                    background: white;
                    border: 1px solid #d1d5db;
                    border-radius: 6px;
                    padding: 8px 10px;
                    font-size: 14px;
                    line-height: 1.4;
                    box-shadow: 0 1px 4px rgba(0,0,0,0.15);
                ">
                    <div style="margin-bottom:4px;"><span style="display:inline-block;width:26px;height:0;border-top:4px solid #b91c1c;vertical-align:middle;margin-right:8px;"></span>HIGH</div>
                    <div style="margin-bottom:4px;"><span style="display:inline-block;width:26px;height:0;border-top:4px solid #f59e0b;vertical-align:middle;margin-right:8px;"></span>MEDIUM</div>
                    <div style="margin-bottom:4px;"><span style="display:inline-block;width:26px;height:0;border-top:4px solid #22c55e;vertical-align:middle;margin-right:8px;"></span>LOW</div>
                </div>
                """
                traffic_map.get_root().html.add_child(folium.Element(legend_html))

                components.html(traffic_map._repr_html_(), height=650, scrolling=False)
                if not path_data:
                    st.info("No live road segments in the latest window. Metrics remain cumulative for this dashboard session.")
            else:
                layers = []
                if path_data:
                    layers.append(
                        pdk.Layer(
                            "PathLayer",
                            data=path_data,
                            get_path="path",
                            get_color="color",
                            get_width="width",
                            width_min_pixels=2,
                            pickable=True,
                        )
                    )

                if anchor_pin_data:
                    point_rows = []
                    for pin in anchor_pin_data:
                        point_rows.append({
                            "street": pin["street"],
                            "position": [float(pin["lon"]), float(pin["lat"])],
                            "color": color_rgb(str(pin.get("level", "LOW")).upper()),
                        })
                    layers.append(
                        pdk.Layer(
                            "ScatterplotLayer",
                            data=point_rows,
                            get_position="position",
                            get_fill_color="color",
                            get_radius=18,
                            radius_min_pixels=4,
                            radius_max_pixels=8,
                            pickable=True,
                        )
                    )

                viewport_points = collect_viewport_points(
                    path_rows=path_data,
                    connector_rows=[],
                    pin_rows=anchor_pin_data,
                )

                if viewport_points:
                    min_lat = min(point[0] for point in viewport_points)
                    max_lat = max(point[0] for point in viewport_points)
                    min_lon = min(point[1] for point in viewport_points)
                    max_lon = max(point[1] for point in viewport_points)
                    center_lat = (min_lat + max_lat) / 2
                    center_lon = (min_lon + max_lon) / 2
                    zoom_level = estimate_zoom_from_bounds(viewport_points)
                else:
                    center_lat = avg_lat
                    center_lon = avg_lon
                    zoom_level = 12.6

                view_state = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=zoom_level, pitch=0)
                deck = pdk.Deck(
                    map_provider="carto",
                    map_style="light",
                    initial_view_state=view_state,
                    layers=layers,
                    tooltip={
                        "html": "<b>{street}</b><br/>Level: {level}<br/>Load: {load}",
                        "style": {"backgroundColor": "#111827", "color": "white"},
                    },
                )
                st.pydeck_chart(deck, width="stretch")
                if not path_data:
                    st.info("No live road segments in the latest window. Metrics remain cumulative for this dashboard session.")

        with decision_col:
            st.subheader("Traffic Decisions")
            if not ranked:
                st.info("No roads available yet.")
            for row in ranked:
                level = str(row.get("level", "LOW")).upper()
                badge_color = "#166534" if level == "LOW" else ("#92400e" if level == "MEDIUM" else "#991b1b")
                load_value = round(float(row.get("load", 0.0)), 3)
                reroute_text = build_reroute_text(row)
                st.markdown(
                    f"<div style='background:{badge_color};color:white;padding:8px 10px;border-radius:8px;margin-bottom:8px;'>"
                    f"<b>{row['street']}</b> • {level} • Load {load_value}<br/><span style='font-size:12px;'>{reroute_text}</span></div>",
                    unsafe_allow_html=True,
                )

    with tab_load:
        st.subheader("Traffic Load Balancing")
        capacity_rows = []
        balance_rows = ranked if ranked else overall_ranked
        for row in balance_rows:
            load_value = float(row.get("load", 0.0))
            used_pct = max(5.0, min(98.0, (load_value / 3.5) * 100.0))
            capacity_rows.append({
                "Street": row["street"],
                "Used": round(used_pct, 1),
                "Available": round(100.0 - used_pct, 1),
            })

        if capacity_rows:
            capacity_df = pd.DataFrame(capacity_rows)
            st.caption("Street Capacity Utilization")
            st.bar_chart(capacity_df.set_index("Street")[["Used", "Available"]], height=320)

        c_opt_1, c_opt_2, c_opt_3 = st.columns(3)
        c_opt_1.metric("Streets Optimized", f"{sum(1 for r in ranked if r.get('reroute_options'))}/{len(balance_rows)}")
        c_opt_2.metric("High Roads", str(high_count))
        c_opt_3.metric("Optimization Level", f"{optimization_label} ({optimization_score:.1f})")

        st.subheader("Optimized Traffic Routes")
        if reroute_guidance_rows:
            option_map = {
                row["congested_road"]: f"{row['reroute_1']} | {row['reroute_2']}"
                for row in reroute_guidance_rows
            }
            selected_road = st.selectbox("Select congested road", list(option_map.keys()))
            st.info(f"Suggested alternatives: {option_map[selected_road]}")
        else:
            st.info("No active reroute required currently.")

    with tab_impact:
        st.subheader("With Rerouting vs Without Rerouting")
        st.caption(
            f"Recency-weighted session comparison across {max(1, int(impact_session.get('samples', 0)))} window(s). "
            "Without-rerouting values are estimated from live congestion and reroute coverage."
        )

        with_col, without_col = st.columns(2)
        with with_col:
            st.markdown("### ✅ With this project (rerouting active)")
            st.metric("Average Speed", f"{impact_session['current_speed_kmh']:.1f} km/h")
            st.metric("Average Delay", f"{impact_session['current_delay']:.1f} mins")
            st.metric("High Roads", f"{impact_session['high_total']:.1f}")
            st.metric("Reroute Coverage (High Roads)", f"{impact_session['reroute_coverage_pct']:.1f}%")
            st.metric("Estimated Vehicle Clogging Index", f"{impact_session['current_clogging_index']:.1f}/100")
            st.metric("Estimated Carbon Emission", f"{impact_session['current_emission_kgph']:.1f} kg CO₂/h")

        with without_col:
            st.markdown("### ⚠️ Without this project (estimated)")
            st.metric("Estimated Average Speed", f"{impact_session['baseline_speed_kmh']:.1f} km/h")
            st.metric("Estimated Average Delay", f"{impact_session['baseline_delay']:.1f} mins")
            st.metric("Estimated High Roads", f"{impact_session['baseline_high_roads']:.1f}")
            st.metric("Estimated Avg Load", f"{impact_session['baseline_load']:.2f}")
            st.metric("Estimated Vehicle Clogging Index", f"{impact_session['baseline_clogging_index']:.1f}/100")
            st.metric("Estimated Carbon Emission", f"{impact_session['baseline_emission_kgph']:.1f} kg CO₂/h")

        d1, d2, d3, d4, d5 = st.columns(5)
        d1.metric("Speed Preserved by Rerouting", f"{impact_session['speed_drop']:.1f} km/h")
        d2.metric("Delay Avoided by Rerouting", f"{impact_session['delay_increase']:.1f} mins")
        d3.metric("Extra High Roads Prevented", f"{impact_session['high_road_increase']:.1f}")
        d4.metric("Clogging Reduced", f"{impact_session['clogging_reduction']:.1f} ({impact_session['clogging_reduction_pct']:.1f}%)")
        d5.metric("Carbon Emission Reduced", f"{impact_session['co2_reduction_kgph']:.1f} kg CO₂/h")

        st.subheader("Why this project is good")
        st.success(
            f"The system detects congestion in real time, proposes alternate roads, and reduces traffic pressure before it spreads. "
            f"Current optimization score is {optimization_score:.1f}% ({optimization_label})."
        )
        st.markdown(
            "\n".join([
                f"- Session rerouting coverage for HIGH roads: **{impact_session['reroute_coverage_pct']:.1f}%**",
                f"- Estimated speed loss avoided: **{impact_session['speed_drop']:.1f} km/h**",
                f"- Estimated delay avoided: **{impact_session['delay_increase']:.1f} mins**",
                f"- Prevents congestion escalation to about **{impact_session['high_road_increase']:.1f}** additional HIGH roads",
                f"- Estimated vehicle clogging reduced by **{impact_session['clogging_reduction']:.1f} points** ({impact_session['clogging_reduction_pct']:.1f}%)",
                f"- Estimated carbon emission reduced by **{impact_session['co2_reduction_kgph']:.1f} kg CO₂/h**",
            ])
        )
        st.info(
            "This comparison is an operational estimate based on current live load and reroute availability, useful for quick decision support."
        )

    with tab_raw:
        st.subheader("Final Result Data")
        raw_rows = []
        for row in streets:
            raw_rows.append({
                "Street": row.get("street", ""),
                "Level": row.get("model_level", row.get("level", "LOW")),
                "Load": round(float(row.get("load", 0.0)), 3),
                "Lat": round(float(row.get("lat", 0.0)), 6),
                "Lon": round(float(row.get("lon", 0.0)), 6),
                "Reroute": ", ".join(row.get("reroute_options", [])) if row.get("reroute_options") else "",
            })

        if raw_rows:
            raw_df = pd.DataFrame(raw_rows)
            st.dataframe(raw_df, width="stretch")
            st.download_button(
                label="Export CSV",
                data=raw_df.to_csv(index=False).encode("utf-8"),
                file_name="traffic_raw_data.csv",
                mime="text/csv",
            )
        else:
            st.info("No raw rows available.")


refresh_seconds = st.sidebar.slider("Auto-refresh (seconds)", min_value=2, max_value=15, value=10)

use_osm_geometry = USE_OSM_GEOMETRY and ox is not None

if st_autorefresh is not None:
    st_autorefresh(interval=refresh_seconds * 1000, key="dashboard-live-refresh")
    render_dashboard(refresh_seconds, use_osm_geometry)
else:
    st.sidebar.caption("Install `streamlit-autorefresh` for automatic updates. Currently using manual refresh.")
    render_dashboard(refresh_seconds, use_osm_geometry)
