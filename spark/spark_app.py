import psycopg2
import redis  # 👈 Added Redis import
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

# --- REDIS CONFIGURATION ---
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_KEY_PREFIX = "latest_iot_reading:"
# We'll only track the latest 50 records in Redis.
LATEST_RECORDS_LIMIT = 50 


# ---------- 2. Redis Update Function ----------
def update_redis_latest(spark_session):
    """
    Reads the latest LATEST_RECORDS_LIMIT records from Postgres and updates Redis.
    This function runs after a batch is written to ensure Redis reflects the absolute latest data.
    """
    try:
        # Read the latest 50 records directly from PostgreSQL
        jdbc_url = "jdbc:postgresql://postgres:5432/iot_db"
        connection_properties = {
            "user": "user",
            "password": "password",
            "driver": "org.postgresql.Driver"
        }

        # Read, sort by timestamp_recorded (desc), and limit
        df_latest = (
            spark_session.read.jdbc(
                url=jdbc_url,
                table="anomalies_raw",
                properties=connection_properties
            )
            .orderBy(col("timestamp_recorded").desc())
            .limit(LATEST_RECORDS_LIMIT)
        )

        # Use the Redis writer function
        df_latest.foreachPartition(write_partition_to_redis)

    except Exception as e:
        print(f"Error reading from Postgres or writing to Redis: {e}")

from datetime import datetime
from decimal import Decimal # Import the Decimal type for checking

# Assuming REDIS_HOST, REDIS_PORT, and REDIS_KEY_PREFIX are defined globally

def write_partition_to_redis(partition):
    """Writes a partition of data (Spark Row objects) to Redis Hash."""
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, socket_connect_timeout=5)
        r.ping() 
    except Exception as e:
        print(f"Could not connect to Redis: {e}")
        return

    for row in partition:
        data_dict = row.asDict()
        
        # --- FIX: Iterate and convert problematic types ---
        for key, value in data_dict.items():
            if isinstance(value, datetime):
                # Convert datetime to ISO string (Fix from previous error)
                data_dict[key] = value.isoformat()
            
            elif isinstance(value, Decimal):
                # Convert Decimal to float (Fix for the current error)
                data_dict[key] = float(value)
                
            # If you needed to preserve precision, you would use:
            # elif isinstance(value, Decimal):
            #     data_dict[key] = str(value)
        # ---------------------------------------------------------------------

        # Create a unique key
        timestamp_str = data_dict.get('timestamp_recorded', 'unknown')
        device_id = data_dict.get('device_id', 'unknown')
        record_key = f"{device_id}:{timestamp_str}"
        redis_key = REDIS_KEY_PREFIX + record_key
        
        # Store as Redis Hash
        r.hset(redis_key, mapping=data_dict)


# ---------- 3. PostgreSQL Sink Function (Modified) ----------
def write_to_postgres(df, epoch_id):
    # --- PostgreSQL Write ---
    records = df.collect()
    if records:
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
            
            for row in records:
                reason = "Marked as anomaly" if row.is_anomaly else "Normal reading"
                # Note: PySpark timestamps are usually in milliseconds, but your PG schema 
                # expects seconds (TO_TIMESTAMP(%s)), so we divide here.
                timestamp_sec = int(row.timestamp / 1000) 
                cur.execute(insert_sql, (
                    row.device_id,
                    timestamp_sec,
                    row.temperature,
                    reason
                ))

            conn.commit()
            cur.close()
            conn.close()
            print(f"Batch {epoch_id}: Successfully committed {len(records)} records to Postgres.")

        except Exception as e:
            print(f"Batch {epoch_id}: Error writing to Postgres: {e}")

    # --- Redis Update (After Postgres write) ---
    # We call the update function to refresh Redis with the latest data from the database
    update_redis_latest(df.sparkSession)


# ---------- 4. Spark Application ----------
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("IoTAnomalyDetector") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ... (Kafka reading and JSON parsing remains the same) ...
    kafka_stream = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "iot_data")
            .option("startingOffsets", "latest")
            .load()
    )

    df = (
        kafka_stream
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
    )

    # --- Stream to Sink ---
    query = (
        df.writeStream
          .foreachBatch(write_to_postgres)
          .outputMode("append") # Use append mode for foreachBatch
          .start()
    )

    query.awaitTermination()