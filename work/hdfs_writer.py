import json

from hdfs import InsecureClient
from kafka import KafkaConsumer

TOPIC = "weather_transformed"
BOOTSTRAP_SERVERS = "kafka:9092"
WEBHDFS = "http://namenode:9870"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: m.decode("utf-8"),
)
client = InsecureClient(WEBHDFS, user="root")

for msg in consumer:
    data = json.loads(msg.value)
    folder = f"/hdfs-data/{data['country']}/{data['city']}"
    path = f"{folder}/alerts.json"
    line = msg.value + "\n"

    client.makedirs(folder)
    if client.status(path, strict=False) is None:
        client.write(path, line)  # 1er message : crée le fichier
    else:
        client.write(path, line, append=True)  # suivants : ajoute à la fin

    print(f"écrit → {path}")
