from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("RDD CSV Filter") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Read CSV as an RDD of strings
rdd = sc.textFile("data/transactions.csv")

# Display each line
rdd.foreach(print)
print("================================")

header = rdd.first()

data_rdd = rdd.filter(lambda row: row != header)

# Convert CSV strings into tuples
transactions_rdd = data_rdd.map(
    lambda row: (
        int(row.split(",")[0]),
        row.split(",")[1],
        float(row.split(",")[2]),
        row.split(",")[3]
    )
)

transactions_rdd.foreach(print)
print("================================")

# Step 4: Filter amount > 10000
filtered_rdd = transactions_rdd.filter(
    lambda row: row[2] > 10000
)

# Step 5: Display result
for record in filtered_rdd.collect():
    print(record)

input("Press Enter to stop the Spark application...")
spark.stop()