# Used in Jupyter to verify what was in the csv.
#import pandas as pd
#df = pd.read_csv('/content/orders_20260324_143042.csv')
#df.head(5)
#df.dtypes


# transform.py
# Read in a csv, perform simple transformations and save processed csv.

from pyspark.sql.functions import regexp_replace, col, concat, lit, round
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("TransformOrders") \
    .getOrCreate()

file = "orders_20260324_143042.csv"
path = os.path.join("/content/", file)


df = spark.read.csv(f"{path}", header=True, inferSchema=True)
#df.printSchema()
#df.show(5)

# Transform the data
# first withColumn not needed unless presenting and want 2 0's to properly represent currency.
# round() could go to 2 decimal places to also properly represent currency.
df = (
    df.withColumn("UnitPrice", concat(col("UnitPrice"), lit("0"))) \
    .withColumn("UnitPrice", col("UnitPrice").cast(FloatType())) \
    .withColumn("TotalPrice", col("Quantity") * col("UnitPrice")) \
    .withColumn("TotalPrice", round(col("TotalPrice"), 1)) \
    .withColumn("CustomerName", regexp_replace(col("CustomerName"),
                                               r"([a-z])([A-Z])", r"\1 \2"))
    )

#Save for future in case a "$" sign is needed.
#df = df.withColumn(
   # "TotalPriceFormatted",
    #format_string("$%.2f", col("TotalPrice"))
#)


# Clean what we have in this simple example but dropping any NA observations.
df = df.dropna()
df.show(truncate=False)


# Save the process dataframe to be loaded into the designated storage location.
#os.makedirs("data/processed", exist_ok=True)
#output_path = f"data/processed/processed_{latest_file}"
output_path = f"/content/processed_orders"
output_dir = "output"

# Force output into 1 partition !!!Be careful, this will overwrite the output_dir with this partition!!!
df.coalesce(1).write.mode("overwrite").csv(output_dir, header=True)

# Rename this file from the part-xxxxx used by PySpark partitions to a human-friendly name.
file_name = [f for f in os.listdir(output_dir) if f.startswith("part-")][0]
os.rename(f"{output_dir}/{file_name}", f"{output_dir}/orders_processed.csv")
print(f"Saved: {output_path}")

