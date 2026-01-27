
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Arg Parser to read input arguments from command line/terminal
parser = argparse.ArgumentParser(description='Most popular listings parameters')
parser.add_argument('--listings', help='Path to the listings dataset') # --listings command-line argument is stored in args.listings
parser.add_argument('--reviews', help='Path to the reviews dataset')
parser.add_argument('--output', help='Directory to save the output')
args = parser.parse_args()

# Start Spark Session
spark = SparkSession.builder \
    .appName("Most popular listings") \
    .getOrCreate()

# Read the data sets in
listings = spark.read.csv(args.listings,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

reviews = spark.read.csv(args.reviews,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)


# Join the data sets together
listings_reviews = listings.join(
    reviews, listings.id == reviews.listing_id, how='inner'
)

# Perform a GroupBy
reviews_per_listing = listings_reviews \
  .groupBy(listings.id, listings.name) \
  .agg(
    F.count(reviews.id).alias('num_reviews')
  ) \
  .orderBy('num_reviews', ascending=False) \

# Write the result to a .csv file
reviews_per_listing \
  .write \
  .csv(args.output)