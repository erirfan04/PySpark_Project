from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDDExample") \
    .master("local[2]") \
    .getOrCreate()

sc = spark.sparkContext

data = [
    ("Irfan", 100000),
    ("Rahul", 80000),
    ("Amit", 90000)
]

rdd = sc.parallelize(data)

print(rdd.collect())
high_salary = rdd.filter(lambda x: x[1] > 85000)

print(high_salary.collect())

input("Press Enter to stop the Spark application...")