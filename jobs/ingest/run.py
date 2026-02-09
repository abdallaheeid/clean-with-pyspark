from pyspark.sql import functions as F
from jobs.spark_session import get_spark
from pyspark.sql.types import TimestampType, DoubleType, LongType
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os
import json
from functools import reduce

load_dotenv()

RAW_PATH = os.getenv("RAW_PATH")
STAGING_PATH = os.getenv("STAGING_PATH")
INGESTION_METRICS_PATH = os.getenv("INGESTION_METRICS_PATH")


def main():
    spark = get_spark("nyc-taxi-ingest")

    files = sorted(Path(RAW_PATH).glob("*.parquet")) # type: ignore

    dfs = []
    rows_read = 0

    for f in files:
        df = spark.read.parquet(str(f))

        rows_read += df.count()

        df = (
            df.select(
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "fare_amount"
            )
            .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
            .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
            .withColumn("passenger_count", F.col("passenger_count").cast(LongType()))
            .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        dfs.append(df)

    df_staging = reduce(lambda a, b: a.unionByName(b), dfs)

    df_staging.write.mode("overwrite").parquet(STAGING_PATH)

    rows_written = df_staging.count()

    metrics = {
        "dataset": "nyc_taxi",
        "rows_read": rows_read,
        "rows_written": rows_written,
        "columns": len(df_staging.columns),
        "ingestion_timestamp": datetime.now().isoformat()
    }

    Path(INGESTION_METRICS_PATH).parent.mkdir(parents=True, exist_ok=True) # type: ignore
    with open(INGESTION_METRICS_PATH, "w") as f: # type: ignore
        json.dump(metrics, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
