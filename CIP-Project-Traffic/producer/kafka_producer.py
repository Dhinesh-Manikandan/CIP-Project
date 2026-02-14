import pandas as pd
import json
import time
import yaml
import os
import shutil
import logging
import kagglehub

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

KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_SERVER = "localhost:9092"  # ✅ HOST ACCESS

logging.info(f"Kafka broker: {KAFKA_SERVER}")

# ================= ENSURE TOPIC =================
admin = KafkaAdminClient(
    bootstrap_servers=KAFKA_SERVER,
    client_id="cip-producer-admin",
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

# ================= DATASET =================
DATA_DIR = os.path.join(BASE_DIR, "..", "data", "us_traffic_congestions")
os.makedirs(DATA_DIR, exist_ok=True)

csv_file = None
for root, _, files in os.walk(DATA_DIR):
    for file in files:
        if file.endswith(".csv"):
            csv_file = os.path.join(root, file)
            break

if csv_file is None:
    dataset_path = kagglehub.dataset_download(
        "sobhanmoosavi/us-traffic-congestions-2016-2022"
    )
    for root, _, files in os.walk(dataset_path):
        for file in files:
            if file.endswith(".csv"):
                shutil.copy(os.path.join(root, file), DATA_DIR)
                csv_file = os.path.join(DATA_DIR, file)
                break

logging.info(f"Dataset loaded: {csv_file}")

# ================= STREAMING =================
CHUNK_SIZE = 20
IMPORTANT_COLS = [
    "Street",
    "Severity",
    "Start_Lat",
    "Start_Lng",
    "Visibility(mi)"
]

def main():
    batch = 0
    try:
        for chunk in pd.read_csv(
            csv_file,
            usecols=IMPORTANT_COLS,
            chunksize=CHUNK_SIZE
        ):
            chunk = (
                chunk.dropna()
                .rename(columns={"Visibility(mi)": "Visibility"})
            )

            for record in chunk.to_dict(orient="records"):
                producer.send(KAFKA_TOPIC, record)

            producer.flush()
            batch += 1
            logging.info(f"✅ Sent batch {batch} ({len(chunk)} records)")
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info("🛑 Producer stopped manually")

    finally:
        producer.close()

if __name__ == "__main__":
    main()
