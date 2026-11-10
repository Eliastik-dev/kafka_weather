# Kafka Météo

Open-Meteo → Kafka → Spark → HDFS → graphiques  
Villes : Paris, Séville, Dubaï, Reykjavik, Wellington (toutes les 30 s).

## Setup

```bash
docker compose up -d --build
```

Créer les topics (1 fois) :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather_stream --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather_transformed --partitions 1 --replication-factor 1
```

## Lancer (1 terminal par script)

```bash
# producteur
docker compose exec pyspark-notebook python /home/jovyan/work/current_weather.py

# alertes Spark → weather_transformed
docker compose exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip pyspark-notebook python /home/jovyan/work/weather_transform.py

# agrégats (fenêtre 1 min)
docker compose exec -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip pyspark-notebook python /home/jovyan/work/weather_aggregates.py

# écriture HDFS
docker compose exec pyspark-notebook python /home/jovyan/work/hdfs_writer.py

# graphiques → work/figures/
docker compose exec pyspark-notebook python /home/jovyan/work/visualize.py
```

`consumer.py` : lecture brute de `weather_stream` (exo 2).

## Scripts

| Fichier | Rôle |
| --- | --- |
| `current_weather.py` | API → `weather_stream` |
| `consumer.py` | affiche `weather_stream` |
| `weather_transform.py` | alertes → `weather_transformed` |
| `weather_aggregates.py` | agrégats console |
| `hdfs_writer.py` | `/hdfs-data/{country}/{city}/alerts.json` |
| `visualize.py` | graphiques |

## Liens

- Jupyter : http://localhost:8888  
- Spark UI : http://localhost:4040  
- HDFS : http://localhost:9870  
