import json
import time
import yaml
import os
import logging
import math
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ================= CONFIG =================
CONFIG_PATH = os.path.join(BASE_DIR, "..", "config", "config.yaml")
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# ✅ Same topic as consumer
KAFKA_TOPIC = config["kafka"]["topic"]   # "traffic-data"
KAFKA_SERVER = "localhost:9092"

logging.info(f"Kafka broker: {KAFKA_SERVER}")
logging.info(f"Using topic: {KAFKA_TOPIC}")

# ================= ENSURE TOPIC =================
admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVER,
    client_id="cip-producer-admin-v2",
    request_timeout_ms=20000
)

try:
    admin.create_topics([
        NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=3,
            replication_factor=1
        )
    ])
    logging.info(f"Created topic: {KAFKA_TOPIC}")
except TopicAlreadyExistsError:
    logging.info(f"Topic already exists: {KAFKA_TOPIC}")
finally:
    admin.close()

# ================= PRODUCER =================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=10
)

STREAM_INTERVAL_SECONDS = 2
PROBES_PER_ROAD = 5
BATCH_SIZE = 20
SYNTH_DATASET_PATH = os.path.join(BASE_DIR, "..", "data", "synthetic_junction_chennai.json")


def load_synthetic_layout(dataset_path):
    with open(dataset_path, "r", encoding="utf-8") as file:
        raw = json.load(file)

    root = raw.get("junction_data", raw)

    roads = root.get("roads", [])
    road_layout = {}
    for entry in roads:
        name = entry["name"]
        coords = [
            (float(lat), float(lon))
            for lat, lon in entry.get("coords", [])
        ]
        road_layout[name] = {
            "coords": coords,
            "base": float(entry.get("base", 2.0)),
            "phase": float(entry.get("phase", 0.0)),
        }

    connections = {
        road_name: list(neighbors)
        for road_name, neighbors in root.get("connections", {}).items()
    }

    simulation = root.get("simulation", {})

    return {
        "junction_id": root.get("junction_id", "synthetic_junction"),
        "road_layout": road_layout,
        "connections": connections,
        "simulation": {
            "incident_duration_steps": max(1, int(simulation.get("incident_duration_steps", 3))),
            "recovery_duration_steps": max(1, int(simulation.get("recovery_duration_steps", 4))),
            "road_cooldown_steps": max(0, int(simulation.get("road_cooldown_steps", 2))),
            "active_incident_boost": float(simulation.get("active_incident_boost", 0.95)),
            "neighbor_reroute_boost": float(simulation.get("neighbor_reroute_boost", 0.30)),
            "active_recovery_penalty": float(simulation.get("active_recovery_penalty", -0.50)),
        },
    }


SYNTH_LAYOUT = load_synthetic_layout(SYNTH_DATASET_PATH)
JUNCTION_ID = SYNTH_LAYOUT["junction_id"]
ROAD_LAYOUT = SYNTH_LAYOUT["road_layout"]
CONNECTED_ROADS = SYNTH_LAYOUT["connections"]
SIMULATION_PROFILE = SYNTH_LAYOUT["simulation"]
ROAD_ORDER = list(ROAD_LAYOUT.keys())


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def initialize_active_cycle(sim_state, road_name):
    rng = sim_state["rng"]

    base_incident = max(1, int(SIMULATION_PROFILE["incident_duration_steps"]))
    base_recovery = max(1, int(SIMULATION_PROFILE["recovery_duration_steps"]))

    # Keep short, jittered windows so primary congestion rotates frequently.
    incident_steps = max(1, min(3, base_incident + rng.randint(-1, 0)))
    recovery_steps = max(1, min(3, base_recovery + rng.randint(-2, -1)))

    sim_state["last_road"] = road_name
    sim_state["active_road"] = road_name
    sim_state["phase"] = "incident"
    sim_state["phase_step"] = 0
    sim_state["current_incident_steps"] = incident_steps
    sim_state["current_recovery_steps"] = recovery_steps


def next_active_congested_road(sim_state):
    if not ROAD_ORDER:
        return ""

    active_road = sim_state.get("active_road")
    phase = sim_state.get("phase")
    phase_step = sim_state.get("phase_step", 0)
    rng = sim_state["rng"]

    incident_duration = int(sim_state.get("current_incident_steps", SIMULATION_PROFILE["incident_duration_steps"]))
    recovery_duration = int(sim_state.get("current_recovery_steps", SIMULATION_PROFILE["recovery_duration_steps"]))
    road_cooldown = SIMULATION_PROFILE["road_cooldown_steps"]

    if active_road:
        phase_step += 1

        # Random early handoff avoids long repeated "primary congestion" bunches.
        if phase_step >= 1 and rng.random() < 0.34:
            sim_state["cooldown"][active_road] = max(1, int(road_cooldown))
            sim_state["active_road"] = ""
            sim_state["phase"] = "idle"
            sim_state["phase_step"] = 0
        else:
            if phase == "incident" and phase_step >= incident_duration:
                sim_state["phase"] = "recovery"
                sim_state["phase_step"] = 0
                return active_road

            if phase == "recovery" and phase_step >= recovery_duration:
                sim_state["active_road"] = ""
                sim_state["phase"] = "idle"
                sim_state["phase_step"] = 0
                sim_state["cooldown"][active_road] = road_cooldown
            else:
                sim_state["phase_step"] = phase_step
                return active_road

    cooldown = sim_state["cooldown"]

    for road_name in ROAD_ORDER:
        if cooldown.get(road_name, 0) > 0:
            cooldown[road_name] -= 1

    candidates = [
        road_name
        for road_name in ROAD_ORDER
        if cooldown.get(road_name, 0) <= 0
    ]
    if not candidates:
        candidates = ROAD_ORDER[:]

    selected = rng.choice(candidates)

    previous = sim_state.get("last_road", "")
    if len(candidates) > 1 and selected == previous:
        alternatives = [road for road in candidates if road != previous]
        selected = rng.choice(alternatives)

    initialize_active_cycle(sim_state, selected)
    return selected


def target_severity(road_name, step_index, active_road, sim_phase):
    spec = ROAD_LAYOUT[road_name]
    road_phase = spec["phase"]

    daily_wave = 0.45 * math.sin((2 * math.pi * step_index / 180.0) + road_phase)
    local_flow = 0.35 * math.sin((2 * math.pi * step_index / 40.0) + (road_phase * 1.7))
    pulse_wave = 0.22 * math.sin((2 * math.pi * step_index / 15.0) + (road_phase * 2.4))

    jam_boost = 0.0
    reroute_lift = 0.0
    recovery_penalty = 0.0
    if road_name == active_road:
        if sim_phase == "incident":
            jam_boost = SIMULATION_PROFILE["active_incident_boost"]
        elif sim_phase == "recovery":
            recovery_penalty = SIMULATION_PROFILE["active_recovery_penalty"]
    elif sim_phase == "incident" and road_name in CONNECTED_ROADS.get(active_road, []):
        reroute_lift = SIMULATION_PROFILE["neighbor_reroute_boost"]

    severity = spec["base"] + daily_wave + local_flow + pulse_wave + jam_boost + reroute_lift + recovery_penalty
    return clamp(severity, 1.0, 3.6)


def allocate_random_probe_counts(rng, roads, target_total, min_per_road=1):
    if not roads:
        return {}

    min_per_road = max(0, int(min_per_road))
    baseline_total = min_per_road * len(roads)
    if target_total < baseline_total:
        target_total = baseline_total

    counts = {road: min_per_road for road in roads}
    remaining = target_total - baseline_total
    if remaining <= 0:
        return counts

    # Weighted random split so some roads get denser probes per window.
    raw_weights = []
    for _ in roads:
        raw_weights.append(0.65 + (rng.random() * 1.35))

    total_weight = sum(raw_weights)
    if total_weight <= 1e-9:
        total_weight = 1.0

    fractional = []
    assigned = 0
    for index, road in enumerate(roads):
        exact = remaining * (raw_weights[index] / total_weight)
        integer_part = int(exact)
        counts[road] += integer_part
        assigned += integer_part
        fractional.append((exact - integer_part, road))

    leftovers = remaining - assigned
    if leftovers > 0:
        fractional.sort(key=lambda item: item[0], reverse=True)
        for i in range(leftovers):
            counts[fractional[i % len(fractional)][1]] += 1

    return counts


def severity_anchor_for_level(level, rng):
    if level == "LOW":
        return 1.35 + (rng.random() * 0.35)
    if level == "HIGH":
        return 3.35 + (rng.random() * 0.60)
    return 2.10 + (rng.random() * 0.55)


def choose_balanced_level_plan(roads, step_index, rng, active_road, active_phase):
    plan = {}
    if not roads:
        return plan

    shuffled = roads[:]
    rng.shuffle(shuffled)

    base_levels = ["LOW", "MEDIUM", "HIGH"]
    for idx, road in enumerate(shuffled):
        plan[road] = base_levels[idx % len(base_levels)]

    if active_road:
        if active_phase == "incident":
            plan[active_road] = "HIGH"
        elif active_phase == "recovery":
            plan[active_road] = "LOW"

    return plan


def build_synthetic_batch(step_index, smoothed_state, sim_state):
    batch = []
    rng = sim_state["rng"]
    active_road = next_active_congested_road(sim_state)
    active_phase = sim_state.get("phase", "incident")
    if not active_road:
        return batch, active_road

    target_level_plan = choose_balanced_level_plan(
        roads=ROAD_ORDER,
        step_index=step_index,
        rng=rng,
        active_road=active_road,
        active_phase=active_phase,
    )

    # Update all roads gradually and steer each one toward a balanced LOW/MEDIUM/HIGH
    # target so every window shows a realistic color mix.
    for road_name in ROAD_ORDER:
        spec = ROAD_LAYOUT[road_name]
        dynamic_target = target_severity(road_name, step_index, active_road, active_phase)
        level_target = severity_anchor_for_level(target_level_plan.get(road_name, "MEDIUM"), rng)
        target = (0.35 * dynamic_target) + (0.65 * level_target)
        previous = smoothed_state.get(road_name, spec["base"])

        smoothed = (0.65 * previous) + (0.35 * target)
        smoothed_state[road_name] = smoothed

    # Randomize total records around BATCH_SIZE to avoid rigid cadence.
    spread_ratio = 0.45
    low = max(len(ROAD_ORDER), int(BATCH_SIZE * (1.0 - spread_ratio)))
    high = max(low + 1, int(BATCH_SIZE * (1.0 + spread_ratio)))
    target_batch_size = rng.randint(low, high)
    probes_by_road = allocate_random_probe_counts(
        rng=rng,
        roads=ROAD_ORDER,
        target_total=target_batch_size,
        min_per_road=1,
    )

    for road_name in ROAD_ORDER:
        spec = ROAD_LAYOUT[road_name]
        smoothed = smoothed_state[road_name]
        coords = spec["coords"]
        if not coords:
            continue
        if len(coords) == 1:
            coords = [coords[0], coords[0]]

        segment_count = max(1, len(coords) - 1)

        road_probe_count = probes_by_road.get(road_name, 1)
        for record_idx in range(road_probe_count):
            probe_id = rng.randint(0, max(0, PROBES_PER_ROAD - 1))
            t = rng.random()

            seg_pos = t * segment_count
            seg_idx = min(segment_count - 1, int(seg_pos))
            local_t = seg_pos - seg_idx

            lat1, lon1 = coords[seg_idx]
            lat2, lon2 = coords[seg_idx + 1]
            lat = lat1 + ((lat2 - lat1) * local_t)
            lon = lon1 + ((lon2 - lon1) * local_t)

            probe_wave = 0.14 * math.sin((2 * math.pi * (step_index + probe_id) / 36.0) + spec["phase"])

            # Pre-send stochastic shift to force a visible LOW/MEDIUM/HIGH mix.
            # Only increasing severity would suppress GREEN; keep both upward and downward moves.
            random_bucket = rng.random()
            if random_bucket < 0.28:
                random_shift = -0.95 + (rng.random() * 0.55)   # more likely GREEN windows
            elif random_bucket < 0.68:
                random_shift = -0.18 + (rng.random() * 0.38)   # MEDIUM windows
            else:
                random_shift = 0.58 + (rng.random() * 0.85)    # RED windows

            probe_severity = clamp(smoothed + probe_wave + random_shift, 1.0, 4.0)

            normalized = (probe_severity - 1.0) / 3.0
            delay = round(1.5 + (normalized * 7.5), 2)
            visibility = round(9.5 - (normalized * 2.8), 2)

            batch.append({
                "Street": road_name,
                "Severity": round(probe_severity, 3),
                "Start_Lat": round(lat, 6),
                "Start_Lng": round(lon, 6),
                "Visibility": visibility,
                "Delay": delay,
                "ProbeId": probe_id,
                "simulation_ts": datetime.utcnow().isoformat(),
                "junction_id": JUNCTION_ID,
            })

    rng.shuffle(batch)

    return batch, active_road


# ================= STREAMING =================
def main():
    batch = 0
    step_index = 0
    smoothed_state = {}
    simulation_state = {
        "rng": random.Random(),
        "last_road": "",
        "active_road": "",
        "phase": "idle",
        "phase_step": 0,
        "cooldown": {road: 0 for road in ROAD_ORDER},
    }
    logging.info(f"Starting synthetic junction simulation for {JUNCTION_ID}")

    try:
        while True:
            batch_records, active = build_synthetic_batch(step_index, smoothed_state, simulation_state)

            for record in batch_records:
                producer.send(KAFKA_TOPIC, record)

            producer.flush()
            batch += 1
            logging.info(
                f"✅ Sent {batch} ({len(batch_records)} records) | "
                f"primary congestion: {active} ({simulation_state.get('phase', 'incident')})"
            )
            step_index += 1
            time.sleep(STREAM_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("🛑 Corridor Producer stopped manually")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
