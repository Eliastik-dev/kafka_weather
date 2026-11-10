import json
import os

import matplotlib.pyplot as plt
import pandas as pd
from hdfs import InsecureClient

WEBHDFS = "http://namenode:9870"
OUT = "/home/jovyan/work/figures"
os.makedirs(OUT, exist_ok=True)

client = InsecureClient(WEBHDFS, user="root")

rows = []
for country in client.list("/hdfs-data"):
    for city in client.list(f"/hdfs-data/{country}"):
        path = f"/hdfs-data/{country}/{city}/alerts.json"
        with client.read(path) as f:
            for line in f.read().decode().splitlines():
                if not line.strip():
                    continue
                data = json.loads(line)
                if "event_time" in data and "city" in data:
                    rows.append(data)

df = pd.DataFrame(rows)
df["event_time"] = pd.to_datetime(df["event_time"])
df = df.sort_values("event_time")

# 1. température
for city in df["city"].unique():
    part = df[df["city"] == city]
    plt.plot(part["event_time"], part["temperature"], label=city)
plt.title("Évolution de la température")
plt.xlabel("temps")
plt.ylabel("°C")
plt.legend()
plt.savefig(f"{OUT}/temperature.png")
plt.clf()

# 2. vent
for city in df["city"].unique():
    part = df[df["city"] == city]
    plt.plot(part["event_time"], part["windspeed"], label=city)
plt.title("Évolution de la vitesse du vent")
plt.xlabel("temps")
plt.ylabel("m/s")
plt.legend()
plt.savefig(f"{OUT}/windspeed.png")
plt.clf()

# 3. alertes par niveau
df["wind_alert_level"].value_counts().sort_index().plot(kind="bar", title="Alertes vent")
plt.ylabel("nombre")
plt.savefig(f"{OUT}/wind_alerts.png")
plt.clf()

df["heat_alert_level"].value_counts().sort_index().plot(kind="bar", title="Alertes chaleur")
plt.ylabel("nombre")
plt.savefig(f"{OUT}/heat_alerts.png")
plt.clf()

# 4. code météo le plus fréquent par pays
print("Code météo le plus fréquent par pays :")
for country in df["country"].unique():
    code = df[df["country"] == country]["weather_code"].mode()[0]
    print(country, "→", int(code))

print("Graphiques sauvegardés dans", OUT)
