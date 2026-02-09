from jobs.spark_session import get_spark


def main():
    spark = get_spark("clean-job")
    print("Clean job started")
    spark.stop()
    

if __name__ == "__main__":
    main()