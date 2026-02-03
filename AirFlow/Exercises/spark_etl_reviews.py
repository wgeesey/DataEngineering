import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer_reviews", required=True, help="Input CSV file path")
    parser.add_argument("--output_path", required=True, help="Output CSV file path")
    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("CustomerReviews") \
        .getOrCreate()

    customer_reviews = spark.read.csv(
        args.customer_reviews,
        header=True,
    )

    customer_reviews = customer_reviews \
        .withColumn("review_score", customer_reviews["review_score"].cast("float"))

    result = customer_reviews \
        .groupBy("listing_id") \
        .agg(
            avg("review_score").alias("avg_review_score")
        )

    result.write.mode("overwrite").csv(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()