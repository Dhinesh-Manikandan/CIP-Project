# CIP-Project

Real-time traffic monitoring and rerouting demo using Kafka + Flink + Streamlit.

## Project Structure

- `CIP-Project-Traffic/producer/` -> Traffic event producer
- `CIP-Project-Traffic/flink_consumer/` -> Flink stream processing + rerouting logic
- `CIP-Project-Traffic/dashboard.py` -> Streamlit dashboard
- `CIP-Project-Traffic/docker-compose.yml` -> Kafka, Zookeeper, Flink services
- `CIP-Project-Traffic/requirements.txt` -> Python dependencies

## Prerequisites

Install these first:

1. Git
2. Python 3.10+ (recommended)
3. Docker Desktop (with Compose enabled)
4. PowerShell (Windows)

## Clone Repository

```powershell
git clone https://github.com/Dhinesh-Manikandan/CIP-Project.git
cd CIP-Project
cd CIP-Project-Traffic
```

## Create Virtual Environment and Install Dependencies

Run in `CIP-Project-Traffic` folder:

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install --upgrade pip
pip install -r .\requirements.txt
```

If activation is blocked by policy, run once in PowerShell (as needed):

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Start Infrastructure (Kafka + Flink)

```powershell
docker compose up -d
```

Expected containers:

- `zookeeper`
- `kafka`
- `kafka-init`
- `jobmanager`
- `taskmanager`

To verify:

```powershell
docker compose ps
```

## Run Pipeline (Correct Order)

Use **separate terminals** (keep each process running):

### Terminal 1: Activate venv

```powershell
cd <path-to>\CIP-Project\CIP-Project-Traffic
.\venv\Scripts\activate
```

### Terminal 1: Start producer

```powershell
python .\producer\kafka_producer_region_demo.py
```

### Terminal 2: Activate venv and submit Flink job

```powershell
cd <path-to>\CIP-Project\CIP-Project-Traffic
.\venv\Scripts\activate
docker exec -it jobmanager bin/flink run -py /opt/flink/jobs/traffic_consumer_region_demo.py -pyexec python
```

### Terminal 3: Activate venv and run dashboard

```powershell
cd <path-to>\CIP-Project\CIP-Project-Traffic
.\venv\Scripts\activate
streamlit run .\dashboard.py
```

Open dashboard URL shown in terminal (typically):

- `http://localhost:8501`

## Runtime Notes

- Dashboard consumes records from Kafka topic `traffic-decisions`.
- Metrics are session-persistent while Streamlit app is running.
- Producer and Flink job must both be active for live updates.

## Stop Everything

### Stop Streamlit / Producer

Press `Ctrl + C` in their terminals.

## Troubleshooting

### Kafka/Flink not running

```powershell
docker compose ps
docker compose logs -f kafka
docker compose logs -f jobmanager
docker compose logs -f taskmanager
```

### Dashboard not updating

1. Confirm producer terminal is sending batches.
2. Confirm Flink job is submitted and running.
3. Confirm `http://localhost:8081` (Flink UI) shows active job.
4. Refresh dashboard or restart Streamlit.

### Reinstall dependencies cleanly

```powershell
deactivate
Remove-Item -Recurse -Force .\venv
python -m venv venv
.\venv\Scripts\activate
pip install -r .\requirements.txt
```
