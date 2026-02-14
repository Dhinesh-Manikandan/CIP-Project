import json
import joblib
from datetime import datetime

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema
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
MODEL_PATH = "/opt/flink/jobs/rerouting_kmeans.pkl"

WINDOW_SECONDS = 10
PARALLELISM = 1

# ================= CREATE OUTPUT TOPIC IF NOT EXISTS =================
def ensure_topic():
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKERS,
        client_id="flink-dashboard-admin"
    )

    try:
        admin.create_topics([
            NewTopic(
                name=OUTPUT_TOPIC,
                num_partitions=1,
                replication_factor=1
            )
        ])
        print(f"Created topic: {OUTPUT_TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Topic already exists: {OUTPUT_TOPIC}")
    finally:
        admin.close()


# ================= WINDOW FUNCTION =================
class MLBasedRerouting(ProcessWindowFunction):

    def open(self, ctx):
        bundle = joblib.load(MODEL_PATH)
        self.model = bundle["model"]
        self.scaler = bundle.get("scaler")
        self.cluster_labels = bundle.get("cluster_labels", {})

    def process(self, key, context, elements):
        road_stats = {}

        for street, lat, lon, severity in elements:
            road_stats.setdefault(street, {
                "sum": 0,
                "count": 0,
                "lat": lat,
                "lon": lon
            })
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

        for i, street in enumerate(streets):
            cluster = clusters[i]
            level = self.cluster_labels.get(cluster, "LOW")

            result = {
                "street": street,
                "lat": road_stats[street]["lat"],
                "lon": road_stats[street]["lon"],
                "load": float(loads[i][0]),
                "level": level,
                "window_start": datetime.fromtimestamp(
                    context.window().start / 1000
                ).isoformat(),
                "window_end": datetime.fromtimestamp(
                    context.window().end / 1000
                ).isoformat()
            }

            yield json.dumps(result)


# ================= MAIN =================
def main():

    # 🔥 Ensure output topic exists before job starts
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
        "Kafka Source"
    )

    def normalize(x):
        d = json.loads(x)
        return (
            d["Street"],
            float(d["Start_Lat"]),
            float(d["Start_Lng"]),
            float(d["Severity"])
        )

    processed = (
        stream
        .map(normalize, Types.TUPLE([
            Types.STRING(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT()
        ]))
        .key_by(lambda x: 0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(MLBasedRerouting(), Types.STRING())
    )

    processed.sink_to(sink)

    env.execute("Traffic ML → Kafka Dashboard Pipeline")


if __name__ == "__main__":
    main()
