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
conf.set("fs.s3a.connection.timeout", "60000")
conf.set("fs.s3a.connection.establish.timeout", "60000")
conf.set("fs.s3a.retry.interval", "5000")

# Read CSV
try:
    print("Reading CSV from:", args.input_path)
    df = spark.read.csv(args.input_path, header=True, inferSchema=True)
    df.printSchema()
except Exception as e:
    print("Error reading CSV:", str(e))
    spark.stop()
    raise

# Transform
try:
    df_transformed = df.withColumn("order_year", year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date")))
except Exception as e:
    print("Error partitioning columns:", str(e))
    spark.stop()
    raise

# Write Parquet
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
spark.stop()
