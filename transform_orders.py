import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, year, month


def main():
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()


    # Create Spark Session

    spark = SparkSession.builder \
    .appName("WideWorldImportersTransform") \
    .getOrCreate()

    print("Reading CSV from:", args.input_path)

    # Read CSV

    df = spark.read.csv(
        args.input_path,
        header=True,
        inferSchema=True
    )
    df.printSchema()


    # Clean currency colum
    # Ex: "$1,234" -> 1234

    df = df.withColumn(
        "total_amount_clean",
        regexp_replace(col("unit_price"), "[$,]", "")
    )

    df.withColumn(
        "total_amount_clean",
        col("total_amount_clean").cast("double")
    )

    # Partition Columns

    df = df.withColumn("order_year", year("order_date"))
    df = df.withColumn("Order_month", month("order_date"))

    print("Writing to: ", args.output_path)


    # Write to S3 Parquet

    df.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(args.output_path)
    
    # Write to local folder
    #df.write \
    #.mode("overwrite") \
    #.option("header", "True") \
    #.csv(args.output_path)

    spark.stop()


if __name__ == "__main__":
    main()
