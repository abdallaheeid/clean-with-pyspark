from pyspark.sql import functions as F
from jobs.spark_session import get_spark
from pathlib import Path
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

STAGING_PATH = os.getenv("STAGING_PATH")
VALIDATION_METRICS_PATH = os.getenv("VALIDATION_METRICS_PATH")

def main():
    spark = get_spark("Validate-job")
    
    df = spark.read.parquet(STAGING_PATH) # type: ignore
    
    total_rows = df.count()
    
    # --- null counts ---
    null_counts = {
        col: df.filter(F.col(col).isNull()).count() for col in df.columns
    }
    
    # numeric profiling
    numeric_stats = {}
    if "fare_amount" in df.columns:
        stats = df.select(
            F.min("fare_amount").alias("min"),
            F.max("fare_amount").alias("max")).collect()[0]

        numeric_stats["fare_amount"] = {
            "min": stats["min"],
            "max": stats["max"]
        }
        

    # duplicates
    
    duplicate_groups = (
        df.groupBy(df.columns)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    
    # --- invalid values ---
    negative_fares = (
        df.filter(F.col("fare_amount") < 0).count()
        if "fare_amount" in df.columns
        else 0
    )

    report = {
        "dataset": "nyc_taxi",
        "rows": total_rows,
        "null_counts": null_counts,
        "numeric_stats": numeric_stats,
        "duplicate_groups": duplicate_groups,
        "invalid_values": {
            "negative_fares": negative_fares
        },
        "validation_timestamp": datetime.now().isoformat()
    }

    Path(VALIDATION_METRICS_PATH).parent.mkdir(parents=True, exist_ok=True) # type: ignore
    with open(VALIDATION_METRICS_PATH, "w") as f: # type: ignore
        json.dump(report, f, indent=2)

    spark.stop()
    

if __name__ == "__main__":
    main()