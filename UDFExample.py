from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create SparkSession
spark = (
    SparkSession.builder
    .appName("UDFExample")
    .master("local[*]")
    .getOrCreate()
)

# Sample employee data
data = [
    (1, "Amit", 50000),
    (2, "Priya", 80000),
    (3, "Rahul", 120000)
]

df = spark.createDataFrame(
    data,
    ["id", "name", "salary"]
)

df.show()

# Define a custom Python function
def categorize_salary(salary):
    if salary >= 100000:
        return "High"
    elif salary >= 60000:
        return "Medium"
    else:
        return "Low"

# Register Python function as UDF
salary_udf = udf(categorize_salary, StringType())

# Apply UDF
result_df = df.withColumn(
    "salary_category",
    salary_udf(df["salary"])
)

# Display result
result_df.show()

spark.stop()