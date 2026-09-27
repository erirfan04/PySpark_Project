from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("AggregateDemo")
    .master("local[*]")
    .getOrCreate()
)

data = [
    (1, "Ravi", "IT", 60000),
    (2, "Amit", "IT", 80000),
    (3, "Priya", "HR", 50000),
    (4, "Neha", "HR", 70000),
    (5, "John", "IT", 90000),
    (6, "Pooja", "Finance", 75000),
    (7, "Raj", "Finance", 85000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "department", "salary"]
)

df.show()

# Calculate average salary by department
result = (
    df.groupBy("department")
      .agg(avg("salary").alias("average_salary"))
)

result.show()
# Multiple aggregate functions
result = (
    df.groupBy("department")
      .agg(
          count("*").alias("employee_count"),
          sum("salary").alias("total_salary"),
          avg("salary").alias("average_salary"),
          min("salary").alias("minimum_salary"),
          max("salary").alias("maximum_salary")
      )
)

result.show()