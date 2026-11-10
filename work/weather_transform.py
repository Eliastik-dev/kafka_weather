from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

TOPIC_IN = "weather_stream"
TOPIC_OUT = "weather_transformed"
BOOTSTRAP_SERVERS = "kafka:9092"

spark = (
    SparkSession.builder
    .appName("weather-transform")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("current", StructType([
        StructField("temperature_2m", DoubleType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("weather_code", IntegerType()),
    ])),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.select(
        col("timestamp").alias("event_time"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )
    .select(
        "event_time",
        col("data.city").alias("city"),
        col("data.country").alias("country"),
        col("data.current.temperature_2m").alias("temperature"),
        col("data.current.wind_speed_10m").alias("windspeed"),
        col("data.current.weather_code").alias("weather_code"),
    )
    .where(col("city").isNotNull())
)

alerts = parsed.withColumn(
    "wind_alert_level",
    when(col("windspeed") < 10, "level_0")
    .when(col("windspeed") <= 20, "level_1")
    .otherwise("level_2"),
).withColumn(
    "heat_alert_level",
    when(col("temperature") < 25, "level_0")
    .when(col("temperature") <= 35, "level_1")
    .otherwise("level_2"),
)

out = alerts.select(to_json(struct(
    "event_time",
    "temperature",
    "windspeed",
    "city",
    "country",
    "weather_code",
    "wind_alert_level",
    "heat_alert_level",
)).alias("value"))

query = (
    out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/weather_transformed")
    .outputMode("append")
    .start()
)

query.awaitTermination()