# CIP-Project

# ⚙️ Setup & Run Guide

## 1️⃣ Clone the Repository

```bash
git clone <your-repo-url>
cd CIP-Project-Traffic
```

---

## 2️⃣ Create Virtual Environment

### Windows

```bash
python -m venv venv
venv\Scripts\activate
```

### Mac/Linux

```bash
python3 -m venv venv
source venv/bin/activate
```

---

## 3️⃣ Upgrade pip & Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

If requirements.txt is not present:

```bash
pip install pandas kafka-python streamlit folium streamlit-folium joblib pyyaml kagglehub
```

---

## 4️⃣ Build Docker Images (First Time Only)

```bash
docker compose build
```

---

## 5️⃣ Start Kafka + Flink Infrastructure

```bash
docker compose up -d
```

Verify containers are running:

```bash
docker ps
```

Expected containers:

- zookeeper
- kafka
- kafka-init
- jobmanager
- taskmanager

---

## 6️⃣ Verify Kafka Topics

```bash
docker exec -it kafka kafka-topics \
--list \
--bootstrap-server kafka:29092
```

Expected output:

```
traffic-data
```

---

## 7️⃣ Run Kafka Producer 

```bash
python .\producer\kafka_producer.py
```

This sends traffic events to:

```
traffic-data
```

---

## 8️⃣ Run Flink ML Consumer (Inside Docker)

```bash
docker exec -it jobmanager bin/flink run \
-py /opt/flink/jobs/traffic_consumer.py \
-pyexec python
```

This:

- Reads from `traffic-data`
- Applies ML clustering (LOW / MEDIUM / HIGH)

---
## 9️⃣ View Real-Time Output (TaskManager Logs)

```bash
docker logs -f taskmanager
```

# 🛑 Stop the System

To stop containers:

```bash
docker compose down
```



