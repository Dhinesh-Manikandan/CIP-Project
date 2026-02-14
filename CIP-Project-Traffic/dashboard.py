import streamlit as st
import folium
from streamlit_folium import st_folium
from kafka import KafkaConsumer
import json

st.set_page_config(layout="wide")
st.title("🚦 Boston Smart Traffic Corridor Dashboard")

# Kafka consumer
consumer = KafkaConsumer(
    "traffic-decisions",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)

# Poll latest messages (non-blocking)
messages = consumer.poll(timeout_ms=1000)

# Create base map
m = folium.Map(location=[42.34, -71.08], zoom_start=13)

color_map = {
    "LOW": "green",
    "MEDIUM": "orange",
    "HIGH": "red"
}

# Add markers if messages exist
for tp, msgs in messages.items():
    for msg in msgs:
        data = msg.value

        color = color_map.get(data["level"], "blue")

        folium.CircleMarker(
            location=[data["lat"], data["lon"]],
            radius=8,
            color=color,
            fill=True,
            fill_color=color,
            popup=f"""
            <b>Street:</b> {data['street']}<br>
            <b>Load:</b> {data['load']:.2f}<br>
            <b>Level:</b> {data['level']}
            """
        ).add_to(m)

st_folium(m, width=1200, height=700)
