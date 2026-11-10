from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

TOPIC = "weather_transformed"
BOOTSTRAP_SERVERS = "kafka:9092"

spark = SparkSession.builder.appName("weather-aggregates").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
])

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

agg = (
    events
    .withColumn("wind_l1", F.when(F.col("wind_alert_level") == "level_1", 1).otherwise(0))
    .withColumn("wind_l2", F.when(F.col("wind_alert_level") == "level_2", 1).otherwise(0))
    .withColumn("heat_l1", F.when(F.col("heat_alert_level") == "level_1", 1).otherwise(0))
    .withColumn("heat_l2", F.when(F.col("heat_alert_level") == "level_2", 1).otherwise(0))
    .withColumn(
        "total_alerts",
        F.when(
            F.col("wind_alert_level").isin("level_1", "level_2")
            | F.col("heat_alert_level").isin("level_1", "level_2"),
            1,
        ).otherwise(0),
    )
    .withWatermark("event_time", "1 minute")
    .groupBy(F.window("event_time", "1 minute", "30 seconds"), "country", "city")
    .agg(
        F.sum("wind_l1").alias("wind_level_1"),
        F.sum("wind_l2").alias("wind_level_2"),
        F.sum("heat_l1").alias("heat_level_1"),
        F.sum("heat_l2").alias("heat_level_2"),
        F.sum("total_alerts").alias("total_alerts"),
        F.avg("temperature").alias("temp_avg"),
        F.min("temperature").alias("temp_min"),
        F.max("temperature").alias("temp_max"),
    )
)

(
    agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "/home/jovyan/work/checkpoints/weather_aggregates")
    .start()
    .awaitTermination()
)
