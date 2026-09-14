from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VersionCheck")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)