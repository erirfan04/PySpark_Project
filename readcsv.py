from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("CSVDataFrameDemo") \
    .master("local[2]") \
    .getOrCreate()

# Read CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/sample.csv")

    # df = spark.read.csv("data/sample.csv")

# Display data
df.show()

# Display schema
df.printSchema()


input("Press Enter to stop the Spark application...")
spark.stop()