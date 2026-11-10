from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

TOPIC_IN = "weather_stream"
TOPIC_OUT = "weather_transformed"
BOOTSTRAP_SERVERS = "kafka:9092"

spark = SparkSession.builder.appName("weather-transform").master("local[*]").getOrCreate()
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

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", "earliest")
    .load()
    .select(
        F.col("timestamp").alias("event_time"),
        F.from_json(F.col("value").cast("string"), schema).alias("data"),
    )
    .select(
        "event_time",
        F.col("data.city").alias("city"),
        F.col("data.country").alias("country"),
        F.col("data.current.temperature_2m").alias("temperature"),
        F.col("data.current.wind_speed_10m").alias("windspeed"),
        F.col("data.current.weather_code").alias("weather_code"),
    )
    .where(F.col("city").isNotNull())
)

alerts = (
    events
    .withColumn(
        "wind_alert_level",
        F.when(F.col("windspeed") < 10, "level_0")
        .when(F.col("windspeed") <= 20, "level_1")
        .otherwise("level_2"),
    )
    .withColumn(
        "heat_alert_level",
        F.when(F.col("temperature") < 25, "level_0")
        .when(F.col("temperature") <= 35, "level_1")
        .otherwise("level_2"),
    )
)

out = alerts.select(F.to_json(F.struct(
    "event_time", "temperature", "windspeed", "city", "country",
    "weather_code", "wind_alert_level", "heat_alert_level",
)).alias("value"))

(
    out.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/weather_transformed")
    .outputMode("append")
    .start()
    .awaitTermination()
)
