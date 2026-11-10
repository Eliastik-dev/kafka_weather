# Kafka Météo

Pipeline météo en temps réel : l’API [Open-Meteo](https://open-meteo.com/) alimente Kafka, Spark Structured Streaming enrichit chaque relevé avec des alertes de vent et de chaleur, puis republie le résultat sur un second topic.

Cinq villes sont interrogées toutes les 30 secondes : Paris, Séville, Dubaï, Reykjavik et Wellington.

## Architecture

```text
Open-Meteo
    │
    ▼
current_weather.py ──► topic weather_stream ──► weather_transform.py
                              │                          │
                              ▼                          ▼
                         consumer.py            topic weather_transformed
```

Les scripts tournent dans le conteneur Jupyter et parlent à Kafka sur `kafka:9092`. Depuis la machine hôte, le broker est joignable sur `localhost:9094`.

La transformation Spark s’exécute en mode `local[*]` dans le conteneur notebook (le connecteur Kafka y est déjà installé). Le master et le worker Spark du `docker-compose` restent disponibles pour d’autres jobs.

## Prérequis

- Docker et Docker Compose
- Accès sortant vers `api.open-meteo.com`

## Démarrage

À la racine du projet :

```bash
docker compose up -d --build
```

La première construction de l’image PySpark prend quelques minutes. Elle installe `kafka-python`, `requests` et le connecteur `spark-sql-kafka`.

Créer les topics (une seule fois) :

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather_stream --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic weather_transformed --partitions 1 --replication-factor 1
```

## Lancer le pipeline

Trois terminaux, dans cet ordre.

**1. Producteur** — interroge Open-Meteo et publie sur `weather_stream` :

```bash
docker compose exec pyspark-notebook python /home/jovyan/work/current_weather.py
```

**2. Transformation** — lit `weather_stream` depuis le début, calcule les alertes et écrit sur `weather_transformed` :

```bash
docker compose exec pyspark-notebook python /home/jovyan/work/weather_transform.py
```

Le checkpoint est dans `work/checkpoints/weather_transformed`. Pour rejouer le flux depuis le début, arrêter le job et supprimer ce dossier.

**3. Consommateur** — affiche les messages bruts de `weather_stream` :

```bash
docker compose exec pyspark-notebook python /home/jovyan/work/consumer.py
```

`Ctrl+C` arrête chaque script. Les notebooks JupyterLab (`http://localhost:8888`, sans token) peuvent aussi exécuter ces fichiers : le volume `./work` est monté sur `/home/jovyan/work`.

## Messages

Entrée (`weather_stream`) : réponse Open-Meteo, plus `city` et `country`. Les champs utilisés sont `current.temperature_2m` (°C), `current.wind_speed_10m` (m/s) et `current.weather_code`.

Sortie (`weather_transformed`), une ligne JSON :

| Champ | Origine |
| --- | --- |
| `event_time` | Horodatage d’écriture Kafka (`timestamp`) |
| `temperature` | `current.temperature_2m` |
| `windspeed` | `current.wind_speed_10m` |
| `city`, `country`, `weather_code` | Message source |
| `wind_alert_level` | Seuil sur la vitesse du vent |
| `heat_alert_level` | Seuil sur la température |

Alertes :

| Niveau | Vent | Chaleur |
| --- | --- | --- |
| `level_0` | &lt; 10 m/s | &lt; 25 °C |
| `level_1` | 10 à 20 m/s | 25 à 35 °C |
| `level_2` | &gt; 20 m/s | &gt; 35 °C |

## Services

| Service | URL | Rôle |
| --- | --- | --- |
| JupyterLab | http://localhost:8888 | Notebooks et scripts Python |
| Spark UI | http://localhost:4040 | Job Structured Streaming (4041, 4042 pour les suivants) |
| Spark master | http://localhost:8080 | Cluster Spark |
| HDFS | http://localhost:9870 | NameNode |
| Kafka | `localhost:9094` | Broker vu depuis l’hôte |

HDFS (NameNode, DataNode) et YARN sont démarrés avec la stack. Le pipeline actuel s’arrête au topic `weather_transformed`.

## Arrêt

```bash
docker compose down
```

Les volumes HDFS sont conservés. `docker compose down -v` les supprime aussi.

## Arborescence

```text
docker-compose.yml      Kafka, Hadoop, Spark, Jupyter
docker/Dockerfile       Image PySpark commune
hadoop.env              Configuration du cluster Hadoop
work/
  current_weather.py    Producteur Open-Meteo
  consumer.py           Consommateur du topic brut
  weather_transform.py  Alertes Spark vers weather_transformed
```
