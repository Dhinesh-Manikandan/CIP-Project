import os
import pandas as pd
import joblib
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# =====================================================
# PATHS (HOST)
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATASET_CSV = os.path.join(
    BASE_DIR, "..", "data/us_traffic_congestions",
    "us_congestion_2016_2022.csv"
)

MODEL_DIR = os.path.join(BASE_DIR, "..", "flink_consumer")
MODEL_PATH = os.path.join(MODEL_DIR, "rerouting_kmeans.pkl")

SAMPLE_SIZE = 50_000

print(f"📄 Loading dataset from: {os.path.abspath(DATASET_CSV)}")

# =====================================================
# LOAD DATA (MEMORY SAFE)
# =====================================================
chunksize = 200_000
samples = []

for chunk in pd.read_csv(DATASET_CSV, chunksize=chunksize, low_memory=False):
    chunk = chunk[["Street", "Severity"]].dropna()
    if chunk.empty:
        continue

    samples.append(chunk.sample(frac=0.1, random_state=42))
    if sum(len(c) for c in samples) >= SAMPLE_SIZE:
        break

df = pd.concat(samples).sample(
    n=min(SAMPLE_SIZE, sum(len(c) for c in samples)),
    random_state=42
)

print(f"✅ Sample size: {len(df)}")

# =====================================================
# AGGREGATE (MATCH FLINK WINDOW LOGIC – DEMO TUNED)
# =====================================================
print("⚙️ Aggregating traffic by street...")

street_stats = (
    df.groupby("Street")
    .agg(
        avg_severity=("Severity", "mean"),
        vehicle_count=("Severity", "count")
    )
    .reset_index()
)

# 🔥 DEMO FEATURE ENGINEERING
# - log1p reduces dominance of large counts
# - severity² boosts impact of serious congestion
street_stats["traffic_load"] = (
    (street_stats["avg_severity"] ** 2) *
    np.log1p(street_stats["vehicle_count"])
)

X = street_stats[["traffic_load"]].values


# =====================================================
# NORMALIZATION (CRITICAL)
# =====================================================
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# =====================================================
# TRAIN KMEANS (LOW / MEDIUM / HIGH)
# =====================================================
print("🤖 Training KMeans (3 congestion levels)...")

kmeans = KMeans(
    n_clusters=3,
    random_state=42,
    n_init=10
)
kmeans.fit(X_scaled)

# =====================================================
# CLUSTER INTERPRETATION (VERY IMPORTANT)
# =====================================================
centroids = kmeans.cluster_centers_.flatten()
sorted_clusters = np.argsort(centroids)

cluster_labels = {
    sorted_clusters[0]: "LOW",
    sorted_clusters[1]: "MEDIUM",
    sorted_clusters[2]: "HIGH"
}

# =====================================================
# SAVE EVERYTHING
# =====================================================
os.makedirs(MODEL_DIR, exist_ok=True)

joblib.dump(
    {
        "model": kmeans,
        "scaler": scaler,
        "cluster_labels": cluster_labels
    },
    MODEL_PATH
)

print(f"✅ Model saved at: {MODEL_PATH}")
print("📊 Congestion levels:", cluster_labels)
