from pyspark.sql import functions as F
from jobs.spark_session import get_spark
from pyspark.sql.types import TimestampType, LongType, DoubleType
from pathlib import Path
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

STAGING_PATH = os.getenv("STAGING_PATH")
CLEAN_PATH = os.getenv("CLEAN_PATH")
REJECT_PATH = os.getenv("REJECT_PATH")

def main():
    spark = get_spark("nyc-taxi-clean-job")
    
    df = spark.read.parquet(STAGING_PATH) # type: ignore
    
    # --- enforce final schema ---
    df = (
        df
        .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count", F.col("passenger_count").cast(LongType()))
        .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    )
    
    is_valid = (
        F.col("tpep_pickup_datetime").isNotNull() &
        F.col("tpep_dropoff_datetime").isNotNull() &
        (F.col("tpep_pickup_datetime") <= F.col("tpep_dropoff_datetime")) &
        (F.col("passenger_count") >= 0) &
        (F.col("passenger_count") <= 6) &
        (F.col("fare_amount") > 0) &
        (F.col("fare_amount") < 1000)
    )
    
    df_clean = df.filter(is_valid)
    df_reject = df.filter(~is_valid)
    
    # deduplicate clean data
    df_clean = df_clean.dropDuplicates([
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "fare_amount"
    ])
    
    # write clean and reject data
    df_clean.write.mode("overwrite").parquet(CLEAN_PATH)  # type: ignore
    df_reject.write.mode("overwrite").parquet(REJECT_PATH)  # type: ignore
    
    spark.stop()

if __name__ == "__main__":
    main()