from pyspark.sql import functions as F
from jobs.spark_session import get_spark
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import os
import json
from functools import reduce

# --- env
load_dotenv()

RAW_PATH = os.getenv("RAW_PATH")
STAGING_PATH = os.getenv("STAGING_PATH")
METRICS_PATH = os.getenv("METRICS_PATH")


def main():
    spark = get_spark("nyc-taxi-ingest")

    files = sorted(Path(RAW_PATH).glob("*.parquet")) # type: ignore

    dfs = []
    total_rows = 0

    for f in files:
        df = spark.read.parquet(str(f))
        total_rows += df.count()

        df = (
            df
            .select(
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "passenger_count",
                "fare_amount"
            )
            .withColumn("passenger_count", F.col("passenger_count").cast("double"))
            .withColumn("ingestion_ts", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        dfs.append(df)

    df_staging = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        dfs
    ).withColumn("ingestion_ts", F.current_timestamp())

    (
        df_staging.write
        .mode("overwrite")
        .parquet(STAGING_PATH)
    )

    rows_written = df_staging.count()

    metrics = {
        "dataset": "nyc_taxi",
        "rows_read": total_rows,
        "rows_written": rows_written,
        "columns": len(df_staging.columns),
        "ingestion_ts": datetime.now().isoformat()
    }

    Path(METRICS_PATH).parent.mkdir(parents=True, exist_ok=True) # type: ignore
    with open(METRICS_PATH, "w") as f: # type: ignore
        json.dump(metrics, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
