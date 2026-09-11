from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# -----------------------------------
# 1. Create Spark Application / Driver
# -----------------------------------

spark = SparkSession.builder \
    .appName("SparkArchitectureDemo") \
    .master("local[2]") \
    .getOrCreate()


# -----------------------------------
# 2. Create Sample Data
# -----------------------------------

data = [
    ("Irfan", "IT", 50000),
    ("John", "IT", 60000),
    ("David", "IT", 70000),
    ("Sara", "HR", 45000),
    ("Mike", "HR", 55000),
    ("Anna", "HR", 48000)
]

df = spark.createDataFrame(
    data,
    ["name", "department", "salary"]
)


# -----------------------------------
# 3. Transformation
# -----------------------------------

result = df.groupBy("department") \
           .agg(avg("salary").alias("avg_salary"))


# -----------------------------------
# 4. Action
# -----------------------------------

result.show()

input("Press Enter to stop the Spark application...")

# -----------------------------------
# 5. Stop Spark Application
# -----------------------------------

spark.stop()
