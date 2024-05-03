from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3_DATA_SOURCE_PATH = "s3://my-ds-bucket-123/source/data.csv"

S3_DATA_OUTPUT_PATH = "s3://my-ds-bucket-123/output"

def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2").getOrCreate()
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH,header =True)
    print(all_data.count())


if __name__ == "__main__":
    print("Started")
    main()
    