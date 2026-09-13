from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

# 1. Create Spark session
spark = (
    SparkSession.builder
    .appName("OrderAnalysis")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read orders data
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)

print("Original Orders Data:")
df.show()

# 3. Transformations - lazy operations

# Select required columns
selected_df = df.select("OrderID", "Customer", "Category", "Amount")

# Filter only Electronics orders
electronics_df = selected_df.filter(
    selected_df["Category"] == "Electronics"
)

# Group by customer and calculate total amount
total_spend_df = (
    electronics_df
    .groupBy("Customer")
    .agg(spark_sum("Amount").alias("TotalAmount"))
)

# 4. Actions - trigger execution

print("Electronics Orders:")
electronics_df.show()

print("Total Electronics Amount by Customer:")
total_spend_df.show()

print("Number of Electronics Orders:")
print(electronics_df.count())

# Stop Spark session
spark.stop()
