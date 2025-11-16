import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, FloatType, BooleanType
)

# ---------- 1. Define Schema ----------
schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("temperature", FloatType()),
    StructField("is_anomaly", BooleanType()),
    StructField("user", StringType())
])


# ---------- 2. PostgreSQL Sink Function ----------
def write_to_postgres(df, epoch_id):
    records = df.collect()
    if not records:
        return

    try:
        conn = psycopg2.connect(
            host="postgres",
            database="iot_db",
            user="user",
            password="password"
        )
        cur = conn.cursor()

        insert_sql = """
            INSERT INTO anomalies_raw (device_id, timestamp_recorded, temperature, reason)
            VALUES (%s, TO_TIMESTAMP(%s), %s, %s)
        """
        print(insert_sql)

        for row in records:
            reason = "Marked as anomaly" if row.is_anomaly else "Normal reading"
            cur.execute(insert_sql, (
                row.device_id,
                row.timestamp,
                row.temperature,
                reason
            ))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print("Error writing to Postgres:", e)


# ---------- 3. Spark Application ----------
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IoTAnomalyDetector") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # --- Read from Kafka ---
    kafka_stream = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "iot_data")
            .option("startingOffsets", "latest")
            .load()
    )

    # --- Parse JSON ---
    df = (
        kafka_stream
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
    )

    # --- Stream to Postgres ---
    query = (
        df.writeStream
          .foreachBatch(write_to_postgres)
          .start()
    )

    query.awaitTermination()
