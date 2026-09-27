from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = (
    SparkSession.builder
    .appName("ShuffleVsBroadcastJoin")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------------------------------
# Employee DataFrame
# Imagine this is a LARGE DataFrame
# -------------------------------------------------

employee_data = [
    (1, "Ravi", 10),
    (2, "Amit", 20),
    (3, "John", 30),
    (4, "Priya", 10),
    (5, "Neha", 20),
    (6, "Rahul", 30),
    (7, "Pooja", 10)
]

employee_df = spark.createDataFrame(
    employee_data,
    ["emp_id", "name", "dept_id"]
)

# -------------------------------------------------
# Department DataFrame
# Imagine this is a SMALL DataFrame
# -------------------------------------------------

department_data = [
    (10, "IT"),
    (20, "HR"),
    (30, "Finance")
]

department_df = spark.createDataFrame(
    department_data,
    ["dept_id", "department"]
)

print("Employee Data")
employee_df.show()

print("Department Data")
department_df.show()


# =================================================
# EXAMPLE 1: SHUFFLE-BASED JOIN
# =================================================

print("SHUFFLE JOIN")

shuffle_join_df = employee_df.join(
    department_df,
    on="dept_id",
    how="inner"
)

shuffle_join_df.show()

# Check Spark execution plan
shuffle_join_df.explain()


# =================================================
# EXAMPLE 2: BROADCAST JOIN
# =================================================

print("BROADCAST JOIN")

broadcast_join_df = employee_df.join(
    broadcast(department_df),
    on="dept_id",
    how="inner"
)

broadcast_join_df.show()

# Check Spark execution plan
broadcast_join_df.explain()


spark.stop()