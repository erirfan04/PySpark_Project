from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("DataFrame Functions Demo")
    .master("local[*]")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "Ravi", 50000), (2, "Priya", 80000)],
    ["id", "name", "salary"]
)

# Rename all columns
df = df.toDF("emp_id", "emp_name", "salary")
df.show()
df.show(5, truncate=False)

df.printSchema()

print(df.columns)
print(df.dtypes)

print("Total rows:", df.count())

df.describe("salary").show()

df.explain()