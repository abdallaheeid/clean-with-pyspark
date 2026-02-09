from jobs.spark_session import get_spark


def main():
    spark = get_spark("ingest-job")
    print("Ingest job started")
    spark.stop()
    

if __name__ == "__main__":
    main()