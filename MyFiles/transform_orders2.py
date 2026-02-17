import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, year, month



# Parse arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", required=True)
parser.add_argument("--output_path", required=True)
args = parser.parse_args()


# Create Spark Session

spark = SparkSession.builder \
    .appName("WideWorldImportersTransform") \
    .getOrCreate()

conf = spark._jsc.hadoopConfiguration()
print(conf.get("fs.s3a.connection.timeout"))  # returns a string, but should be '60000'
print(conf.get("fs.s3a.connection.establish.timeout"))
print(conf.get("fs.s3a.retry.interval"))


try:
    print("Reading CSV from:", args.input_path)
    df = spark.read.csv(args.input_path, header=True, inferSchema=True)
    df.printSchema()
except Exception as e:
    print("Error reading CSV:", str(e))
    spark.stop()
    raise
try:
    df_transformed = (
    df \
        .withColumn("order_year", year("order_date")) \
        .withColumn("order_month", month("order_month"))
    )   
except Exception as e:
    print("Error partitioning columns:", str(e))
    spark.stop()
    raise
try:
    print("Writing Parquet to:", args.output_path)
    df_transformed.write \
        .mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(args.output_path)
except Exception as e:
    print("Error writing Parquet:", str(e))
    spark.stop()
    raise

print("Transformation Complete")
