import json
import math
import os
import pickle
import time
from datetime import datetime

try:
    import resource
except Exception:
    resource = None

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import WatermarkStrategy

# ================= CONFIG =================
INPUT_TOPIC = "traffic-data"
OUTPUT_TOPIC = "traffic-decisions"
KAFKA_BROKERS = "kafka:29092"

WINDOW_SECONDS = 10
NUM_BUCKETS = 3
PARALLELISM = NUM_BUCKETS
TOP_K_ALTERNATIVES = 2
MAX_REROUTE_DISTANCE_KM = 2.0
MODEL_PATH = "/opt/flink/jobs/rerouting_kmeans.pkl"

ROAD_GRAPH = {
    "Anna Salai": ["Sardar Patel Road", "GST Road", "Inner Ring Road"],
    "Sardar Patel Road": ["Anna Salai", "Inner Ring Road"],
    "GST Road": ["Anna Salai", "Inner Ring Road"],
    "Inner Ring Road": ["Anna Salai", "Sardar Patel Road", "GST Road"],
}


def spatial_bucket(lat: float, lon: float, grid_size=0.02) -> int:
    x = int(lat / grid_size)
    y = int(lon / grid_size)
    return abs(hash((x, y))) % NUM_BUCKETS


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius_km * c


# ================= CREATE OUTPUT TOPIC IF NOT EXISTS =================
def ensure_topic():
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError

        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="flink-dashboard-admin",
        )

        try:
            admin.create_topics([
                NewTopic(
                    name=OUTPUT_TOPIC,
                    num_partitions=PARALLELISM,
                    replication_factor=1,
                )
            ])
            print(f"Created topic: {OUTPUT_TOPIC}")
        except TopicAlreadyExistsError:
            print(f"Topic already exists: {OUTPUT_TOPIC}")
        finally:
            admin.close()
    except Exception as exc:
        print(f"Skipping topic create check: {type(exc).__name__}: {exc}")


# ================= WINDOW FUNCTION =================
class JunctionTrafficRerouting(ProcessWindowFunction):

    def open(self, ctx):
        self.subtask = ctx.get_index_of_this_subtask()
        self.smoothed_load_by_street = {}
        self.model = None
        self.model_label_map = {}
        self.model_n_features = None
        self.started_at = time.time()
        self.windows_processed = 0
        self.last_window_end_epoch = None

        try:
            if os.path.exists(MODEL_PATH):
                with open(MODEL_PATH, "rb") as file:
                    self.model = pickle.load(file)
                self.model_n_features = int(getattr(self.model, "n_features_in_", 1) or 1)
                self.model_label_map = self._build_kmeans_label_map()
                print(f"Subtask {self.subtask}: loaded ML model from {MODEL_PATH}")
            else:
                print(f"Subtask {self.subtask}: ML model not found at {MODEL_PATH}; using fallback rules")
        except Exception as exc:
            self.model = None
            self.model_label_map = {}
            self.model_n_features = None
            print(f"Subtask {self.subtask}: ML model load failed ({type(exc).__name__}: {exc}); using fallback rules")

    @staticmethod
    def level_score(level: str) -> int:
        return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(str(level).upper(), 1)

    @staticmethod
    def classify_level(smoothed_load: float) -> str:
        if smoothed_load >= 3.20:
            return "HIGH"
        if smoothed_load >= 2.40:
            return "MEDIUM"
        return "LOW"

    def _build_kmeans_label_map(self):
        if self.model is None:
            return {}

        centers = getattr(self.model, "cluster_centers_", None)
        if centers is None or len(centers) == 0:
            return {}

        # Prefer smoothed load dimension if available; else first dimension.
        load_index = 1 if len(centers[0]) > 1 else 0
        ordered = sorted(
            [(idx, float(center[load_index])) for idx, center in enumerate(centers)],
            key=lambda x: x[1],
        )

        level_order = ["LOW", "MEDIUM", "HIGH"]
        label_map = {}
        for rank, (cluster_id, _) in enumerate(ordered):
            mapped_level = level_order[min(rank, len(level_order) - 1)]
            label_map[int(cluster_id)] = mapped_level
        return label_map

    def _build_features(self, raw_load, smoothed_load, lat, lon, probe_id):
        base = [
            float(raw_load),
            float(smoothed_load),
            float(lat),
            float(lon),
            float(probe_id),
        ]

        needed = int(self.model_n_features or 1)
        if needed <= len(base):
            return base[:needed]

        # Pad with last known stable signal (smoothed load) if model expects more fields.
        return base + [float(smoothed_load)] * (needed - len(base))

    def predict_level(self, raw_load, smoothed_load, lat, lon, probe_id):
        if self.model is None:
            return self.classify_level(smoothed_load)

        try:
            features = self._build_features(raw_load, smoothed_load, lat, lon, probe_id)
            predicted = self.model.predict([features])[0]

            if isinstance(predicted, str):
                normalized = str(predicted).upper()
                if normalized in {"LOW", "MEDIUM", "HIGH"}:
                    return normalized

            predicted_int = int(predicted)
            if predicted_int in self.model_label_map:
                return self.model_label_map[predicted_int]
        except Exception:
            pass

        return self.classify_level(smoothed_load)

    @staticmethod
    def rebalance_levels_window(entries):
        if not entries:
            return entries

        if len(entries) < 3:
            return entries

        ordered = sorted(entries, key=lambda row: float(row.get("load", 0.0)))
        total = len(ordered)
        low_end = max(1, total // 3)
        med_end = max(low_end + 1, (2 * total) // 3)

        for idx, row in enumerate(ordered):
            if idx < low_end:
                level = "LOW"
            elif idx < med_end:
                level = "MEDIUM"
            else:
                level = "HIGH"

            row["level"] = level
            row["model_level"] = level

        return entries

    def select_nearby_alternatives(self, source, roads_by_name):
        connected = ROAD_GRAPH.get(source["street"], [])
        candidates = []
        fallback_candidates = []
        high_blocked_count = 0
        missing_data_count = 0

        if not connected:
            return [], "No reroute: no connected roads configured"

        for name in connected:
            candidate = roads_by_name.get(name)
            if not candidate:
                missing_data_count += 1
                continue

            candidate_level = candidate["level"]
            if candidate_level == "HIGH":
                high_blocked_count += 1
                continue

            distance_km = haversine_km(
                float(source["lat"]),
                float(source["lon"]),
                float(candidate["lat"]),
                float(candidate["lon"]),
            )
            level_penalty = 0.0 if candidate_level == "LOW" else 0.45
            score = float(candidate["load"]) + level_penalty + (0.35 * distance_km)

            fallback_candidates.append({
                "street": name,
                "distance_km": round(distance_km, 3),
                "score": score,
                "level": candidate_level,
            })

            if distance_km > MAX_REROUTE_DISTANCE_KM:
                continue

            candidates.append({
                "street": name,
                "distance_km": round(distance_km, 3),
                "score": score,
                "level": candidate_level,
            })

        candidates.sort(key=lambda x: x["score"])
        if candidates:
            return candidates[:TOP_K_ALTERNATIVES], f"Reroute via LOW/MEDIUM roads within {MAX_REROUTE_DISTANCE_KM:.1f} km"

        # Fallback: avoid empty reroute when all viable roads are slightly beyond distance limit.
        fallback_candidates.sort(key=lambda x: x["score"])
        if fallback_candidates:
            return (
                fallback_candidates[:TOP_K_ALTERNATIVES],
                f"No LOW/MEDIUM road within {MAX_REROUTE_DISTANCE_KM:.1f} km; using nearest LOW/MEDIUM alternative",
            )

        if high_blocked_count > 0:
            return [], "No reroute: connected roads are HIGH congestion"

        if missing_data_count > 0:
            return [], "No reroute: connected road data unavailable"

        return [], "No reroute candidates available"

    def process(self, key, context, elements):
        started = time.time()
        started_cpu = time.process_time()
        road_stats = {}
        events_processed = 0

        for street, lat, lon, severity, probe_id in elements:
            events_processed += 1
            key_name = f"{street}#{int(probe_id)}"
            road_stats.setdefault(key_name, {
                "street": street,
                "sum": 0.0,
                "count": 0,
                "lat": lat,
                "lon": lon,
                "probe_id": int(probe_id),
            })
            road_stats[key_name]["sum"] += severity
            road_stats[key_name]["count"] += 1
            road_stats[key_name]["lat"] = lat
            road_stats[key_name]["lon"] = lon

        enriched = []
        for key_name, values in road_stats.items():
            street = values["street"]
            raw_load = values["sum"] / max(1, values["count"])
            prev = self.smoothed_load_by_street.get(key_name, raw_load)
            smoothed = (0.35 * prev) + (0.65 * raw_load)
            self.smoothed_load_by_street[key_name] = smoothed

            level = self.predict_level(
                raw_load=raw_load,
                smoothed_load=smoothed,
                lat=values["lat"],
                lon=values["lon"],
                probe_id=values["probe_id"],
            )
            enriched.append({
                "street": street,
                "lat": values["lat"],
                "lon": values["lon"],
                "probe_id": values["probe_id"],
                "load": round(smoothed, 3),
                "raw_load": round(raw_load, 3),
                "level": level,
                "model_level": level,
            })

        # Collapse probe-level rows into one representative row per street.
        # This prevents later rows from the same street overwriting reroute options.
        by_street = {}
        for entry in enriched:
            street = entry["street"]
            current = by_street.get(street)
            if current is None:
                by_street[street] = entry
                continue

            current_score = self.level_score(current["level"])
            entry_score = self.level_score(entry["level"])

            if entry_score > current_score:
                by_street[street] = entry
            elif entry_score == current_score and float(entry["load"]) >= float(current["load"]):
                by_street[street] = entry

        street_enriched = list(by_street.values())
        street_enriched = self.rebalance_levels_window(street_enriched)

        roads_by_name = {entry["street"]: entry for entry in street_enriched}
        sorted_by_load = sorted(street_enriched, key=lambda x: x["load"], reverse=True)

        alternatives_by_road = {}
        alternative_details_by_road = {}
        reroute_reason_by_road = {}
        for item in sorted_by_load:
            if item["level"] == "HIGH":
                nearby_alternatives, reroute_reason = self.select_nearby_alternatives(item, roads_by_name)
                alternatives_by_road[item["street"]] = [x["street"] for x in nearby_alternatives]
                alternative_details_by_road[item["street"]] = nearby_alternatives
                reroute_reason_by_road[item["street"]] = reroute_reason
            else:
                alternatives_by_road[item["street"]] = []
                alternative_details_by_road[item["street"]] = []
                reroute_reason_by_road[item["street"]] = "Low congestion road (LOW/MEDIUM); reroute not required"

        window_start_epoch = context.window().start / 1000
        window_end_epoch = context.window().end / 1000
        window_start = datetime.fromtimestamp(window_start_epoch).isoformat()
        window_end = datetime.fromtimestamp(window_end_epoch).isoformat()

        high_roads = 0
        for item in sorted_by_load:
            if item["level"] == "HIGH":
                high_roads += 1

            result = {
                "record_type": "decision",
                "street": item["street"],
                "lat": item["lat"],
                "lon": item["lon"],
                "load": item["load"],
                "scaled_load": item["load"],
                "level": item["level"],
                "model_level": item["model_level"],
                "probe_id": item.get("probe_id", 0),
                "is_congested": item["level"] == "HIGH",
                "reroute_options": alternatives_by_road[item["street"]],
                "reroute_details": alternative_details_by_road[item["street"]],
                "reroute_reason": reroute_reason_by_road[item["street"]],
                "subtask": self.subtask,
                "window_start": window_start,
                "window_end": window_end,
            }
            yield json.dumps(result)

        latency = time.time() - started
        cpu_time_used = time.process_time() - started_cpu
        cpu_util_pct = (cpu_time_used / max(latency, 1e-6)) * 100.0

        memory_mb = 0.0
        if resource is not None:
            try:
                rss_raw = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                if rss_raw > 10_000_000:
                    memory_mb = rss_raw / (1024.0 * 1024.0)
                else:
                    memory_mb = rss_raw / 1024.0
            except Exception:
                memory_mb = 0.0

        previous_window_end = self.last_window_end_epoch
        window_gap_sec = 0.0 if previous_window_end is None else max(0.0, float(window_end_epoch - previous_window_end))
        self.last_window_end_epoch = float(window_end_epoch)
        self.windows_processed += 1

        uptime_sec = max(1e-6, float(time.time() - self.started_at))
        expected_windows = max(1.0, uptime_sec / float(WINDOW_SECONDS))
        stability_ratio = min(1.0, float(self.windows_processed) / expected_windows)
        estimated_missed_windows = max(0, int(round(expected_windows)) - int(self.windows_processed))
        window_drop_flag = 0
        if previous_window_end is not None and window_gap_sec > (1.5 * float(WINDOW_SECONDS)):
            window_drop_flag = 1

        metrics = {
            "record_type": "metrics",
            "subtask": self.subtask,
            "events_processed": events_processed,
            "unique_roads": len(sorted_by_load),
            "high_roads": high_roads,
            "processing_latency_sec": round(latency, 4),
            "processing_throughput_eps": round(events_processed / max(latency, 1e-6), 3),
            "throughput_eps": round(events_processed / WINDOW_SECONDS, 3),
            "cpu_util_pct": round(cpu_util_pct, 3),
            "memory_mb": round(memory_mb, 3),
            "window_gap_sec": round(window_gap_sec, 3),
            "window_drop_flag": int(window_drop_flag),
            "uptime_sec": round(uptime_sec, 3),
            "windows_processed": int(self.windows_processed),
            "expected_windows": round(expected_windows, 3),
            "stability_ratio": round(stability_ratio, 4),
            "estimated_missed_windows": int(estimated_missed_windows),
            "window_start": window_start,
            "window_end": window_end,
        }
        yield json.dumps(metrics)


# ================= MAIN =================
def main():

    ensure_topic()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(INPUT_TOPIC) \
        .set_group_id("flink-dashboard-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(OUTPUT_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source",
    )

    def normalize(x):
        data = json.loads(x)
        return (
            data["Street"],
            float(data["Start_Lat"]),
            float(data["Start_Lng"]),
            float(data["Severity"]),
            int(data.get("ProbeId", 0)),
        )

    processed = (
        stream
        .map(normalize, Types.TUPLE([
            Types.STRING(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.INT(),
        ]))
        .key_by(lambda x: spatial_bucket(x[1], x[2]), Types.INT())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(JunctionTrafficRerouting(), Types.STRING())
    )

    processed.sink_to(sink)

    env.execute("Traffic ML → Kafka Dashboard Pipeline")


if __name__ == "__main__":
    main()
