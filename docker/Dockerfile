# Image commune : notebook (driver Spark), spark-master, spark-worker.
# Même Spark et même Python partout ; kafka-python et le connecteur Kafka déjà installés.
FROM jupyter/pyspark-notebook:latest

RUN pip install --no-cache-dir kafka-python==2.3.3 requests hdfs==2.7.3

# Connecteur Kafka à la version exacte de Spark et de Scala de l'image (lue dans le nom du jar spark-core_<scala>-<spark>.jar)
USER root
RUN VERSIONS=$(ls "${SPARK_HOME}/jars" | sed -n 's/^spark-core_\([0-9.]*\)-\([0-9.]*\)\.jar$/\1 \2/p') \
 && SCALA_VERSION=${VERSIONS% *} && SPARK_VERSION=${VERSIONS#* } \
 && test -n "${SCALA_VERSION}" && test -n "${SPARK_VERSION}" \
 && echo "spark.jars.packages org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}" >> "${SPARK_HOME}/conf/spark-defaults.conf" \
 && echo "spark.sql.session.timeZone UTC" >> "${SPARK_HOME}/conf/spark-defaults.conf" \
 && cat "${SPARK_HOME}/conf/spark-defaults.conf"
USER ${NB_UID}

# Téléchargement du connecteur à la construction, pas à chaque session
RUN export PYTHONPATH="${SPARK_HOME}/python:$(ls ${SPARK_HOME}/python/lib/py4j-*-src.zip)" \
 && python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').getOrCreate().stop()"
