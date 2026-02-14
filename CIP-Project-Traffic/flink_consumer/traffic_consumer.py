import json
import joblib
import time
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import WatermarkStrategy

# ================= CONFIG =================
KAFKA_TOPIC = "traffic-data"
KAFKA_BROKERS = "kafka:29092"
MODEL_PATH = "/opt/flink/jobs/rerouting_kmeans.pkl"

WINDOW_SECONDS = 10
TOP_K_ALTERNATIVES = 2
NUM_BUCKETS = 3

# ================= SPATIAL PARTITION =================
def spatial_bucket(lat: float, lon: float, grid_size=0.02) -> int:
    x = int(lat / grid_size)
    y = int(lon / grid_size)
    return abs(hash((x, y))) % NUM_BUCKETS

# ================= WINDOW FUNCTION =================
class MLBasedRerouting(ProcessWindowFunction):

    def open(self, ctx):
        bundle = joblib.load(MODEL_PATH)
        self.model = bundle["model"]
        self.scaler = bundle.get("scaler")
        self.cluster_labels = bundle.get("cluster_labels", {})
        self.subtask = ctx.get_index_of_this_subtask()

    def process(self, key, context, elements):
        start = time.time()

        road_stats = {}
        events = 0

        for street, lat, lon, severity in elements:
            events += 1
            road_stats.setdefault(street, {"sum": 0, "count": 0})
            road_stats[street]["sum"] += severity
            road_stats[street]["count"] += 1

        streets, loads = [], []
        for s, v in road_stats.items():
            load = (v["sum"] / v["count"]) * v["count"]
            streets.append(s)
            loads.append([load])

        if self.scaler:
            loads = self.scaler.transform(loads)

        clusters = self.model.predict(loads)

        enriched = list(zip(streets, loads, clusters))
        sorted_by_load = sorted(enriched, key=lambda x: x[1][0])

        # ===== Decisions =====
        for street, load, cluster in enriched:
            label = self.cluster_labels.get(cluster, "UNKNOWN")

            if label == "HIGH":
                alts = [s for s, _, _ in sorted_by_load if s != street][:TOP_K_ALTERNATIVES]
                yield f"[Subtask-{self.subtask}] 🚨 HIGH | {street} | Load={load[0]:.2f} | Reroute → {', '.join(alts)}"

            elif label == "MEDIUM":
                yield f"[Subtask-{self.subtask}] ⚠️ MEDIUM | {street} | Load={load[0]:.2f}"

            else:
                yield f"[Subtask-{self.subtask}] 🟢 LOW | {street} | Load={load[0]:.2f}"

        latency = time.time() - start

        yield (
            f"\n📊 METRICS | Subtask-{self.subtask} | "
            f"Window={datetime.fromtimestamp(context.window().start/1000)} → "
            f"{datetime.fromtimestamp(context.window().end/1000)}\n"
            f"   Events Processed   : {events}\n"
            f"   Unique Roads       : {len(road_stats)}\n"
            f"   Processing Latency : {latency:.3f} sec\n"
            f"   Throughput         : {events/WINDOW_SECONDS:.2f} events/sec\n"
        )

# ================= MAIN =================
def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(NUM_BUCKETS)

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BROKERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink-ml-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka")

    def normalize(x):
        d = json.loads(x)
        return (
            d["Street"],
            float(d["Start_Lat"]),
            float(d["Start_Lng"]),
            float(d["Severity"])
        )

    stream \
        .map(normalize, Types.TUPLE([
            Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT()
        ])) \
        .key_by(lambda x: spatial_bucket(x[1], x[2]), Types.INT()) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS))) \
        .process(MLBasedRerouting(), Types.STRING()) \
        .print()

    env.execute("Spatially-Aware ML Traffic Rerouting")

if __name__ == "__main__":
    main()
