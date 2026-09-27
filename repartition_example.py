from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RepartitionDemo")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (1, "Ravi", "IT"),
    (2, "Amit", "HR"),
    (3, "Priya", "IT"),
    (4, "John", "Finance"),
    (5, "Neha", "HR"),
    (6, "Rahul", "IT"),
    (7, "Anita", "Finance"),
    (8, "Vijay", "IT"),
    (9, "Pooja", "HR"),
    (10, "Raj", "Finance")
]

df = spark.createDataFrame(
    data,
    ["id", "name", "department"]
)

print("Before repartition:",
      df.rdd.getNumPartitions())

# Repartition into 4 partitions
df2 = df.repartition(4)

print("After repartition:",
      df2.rdd.getNumPartitions())

df2.show()