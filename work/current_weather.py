import json
import time

import requests
from kafka import KafkaProducer

TOPIC = "weather_stream"
BOOTSTRAP_SERVERS = "kafka:9092"
INTERVAL_SECONDS = 30

CITIES = [
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Seville", "country": "Spain", "latitude": 37.3891, "longitude": -5.9845},
    {"city": "Dubai", "country": "United Arab Emirates", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Reykjavik", "country": "Iceland", "latitude": 64.1466, "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand", "latitude": -41.2865, "longitude": 174.7762},
]

API_URL = "https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,wind_speed_10m,weather_code&wind_speed_unit=ms"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def fetch_weather(city):
    url = API_URL.format(latitude=city["latitude"], longitude=city["longitude"])
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    payload = response.json()
    payload["city"] = city["city"]
    payload["country"] = city["country"]
    return payload

while True:
    for city in CITIES:
        message = fetch_weather(city)
        producer.send(TOPIC, value=message)
        print(f"envoyé : {message['city']} {message['current']}")
    producer.flush()
    time.sleep(INTERVAL_SECONDS)