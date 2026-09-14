from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DataLakeParquetAvroExample")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-avro_2.12:3.5.7"
    )
    .getOrCreate()
)

# 2. Read CSV file
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/employees.csv")
)
print("Original DataFrame:")
df.show()

# --------------------------------------------------
# Write data to the Data Lake in Parquet format
# --------------------------------------------------

parquet_path = "data_lake/employees/parquet"

df.write \
    .mode("overwrite") \
    .parquet(parquet_path)

print("Data written in Parquet format.")

# --------------------------------------------------
# Write data to the Data Lake in Avro format
# --------------------------------------------------

avro_path = "data_lake/employees/avro"

df.write \
    .mode("overwrite") \
    .format("avro") \
    .save(avro_path)

print("Data written in Avro format.")

# --------------------------------------------------
# Read data from Parquet
# --------------------------------------------------

parquet_df = spark.read.parquet(parquet_path)

print("Data read from Parquet:")
parquet_df.show()

# --------------------------------------------------
# Read data from Avro
# --------------------------------------------------

avro_df = spark.read \
    .format("avro") \
    .load(avro_path)

print("Data read from Avro:")
avro_df.show()

# --------------------------------------------------
# Process the data
# --------------------------------------------------

it_employees = parquet_df.filter(
    parquet_df.department == "IT"
)

print("IT Employees:")
it_employees.show()

input("Press Enter to stop the Spark application...")
spark.stop()