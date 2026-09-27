from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, row_number, rank, dense_rank
from pyspark.sql.window import Window


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = SparkSession.builder \
    .appName("PySpark Window Functions Example") \
    .master("local[*]") \
    .getOrCreate()


# ============================================================
# 2. Create Employee Data
# ============================================================

data = [
    (101, "John", "IT", 90000),
    (102, "Alice", "IT", 120000),
    (103, "Bob", "IT", 120000),
    (104, "David", "IT", 70000),

    (105, "Emma", "HR", 60000),
    (106, "Sophia", "HR", 80000),
    (107, "Mike", "HR", 80000),
    (108, "Tom", "HR", 50000),

    (109, "Robert", "Finance", 100000),
    (110, "James", "Finance", 90000),
    (111, "Linda", "Finance", 90000),
    (112, "Mary", "Finance", 70000)
]


columns = [
    "employee_id",
    "employee_name",
    "department",
    "salary"
]


df = spark.createDataFrame(data, columns)


# ============================================================
# 3. Display Original Data
# ============================================================

print("========== ORIGINAL EMPLOYEE DATA ==========")

df.show()


# ============================================================
# 4. Department Average Salary
# ============================================================

print("========== DEPARTMENT AVERAGE SALARY ==========")


# Create window partitioned by department
window_spec = Window.partitionBy("department")


# Calculate average salary for each department
result_avg = df.withColumn(
    "department_avg_salary",
    avg("salary").over(window_spec)
)


result_avg.show()


# ============================================================
# 5. Ranking Employees Based on Salary
# ============================================================

print("========== EMPLOYEE RANKING ==========")


# Window:
# First divide employees by department
# Then sort salary in descending order

window_spec_rank = (
    Window
    .partitionBy("department")
    .orderBy(col("salary").desc())
)


# ============================================================
# 6. row_number()
# ============================================================

print("========== ROW_NUMBER() ==========")


result_row_number = df.withColumn(
    "row_number",
    row_number().over(window_spec_rank)
)


result_row_number.show()


# ============================================================
# 7. rank()
# ============================================================

print("========== RANK() ==========")


result_rank = df.withColumn(
    "rank",
    rank().over(window_spec_rank)
)


result_rank.show()


# ============================================================
# 8. dense_rank()
# ============================================================

print("========== DENSE_RANK() ==========")


result_dense_rank = df.withColumn(
    "dense_rank",
    dense_rank().over(window_spec_rank)
)


result_dense_rank.show()


# ============================================================
# 9. Compare row_number(), rank(), dense_rank()
# ============================================================

print("========== COMPARISON ==========")


result_comparison = df.withColumn(
    "row_number",
    row_number().over(window_spec_rank)
).withColumn(
    "rank",
    rank().over(window_spec_rank)
).withColumn(
    "dense_rank",
    dense_rank().over(window_spec_rank)
)


result_comparison.orderBy(
    "department",
    col("salary").desc()
).show()


# ============================================================
# 10. Top 2 Highest Paid Employees Per Department
# ============================================================

print("========== TOP 2 EMPLOYEES PER DEPARTMENT ==========")


top_2 = result_row_number.filter(
    col("row_number") <= 2
)


top_2.orderBy(
    "department",
    col("salary").desc()
).show()


# ============================================================
# 11. Highest Paid Employee Per Department
# ============================================================

print("========== HIGHEST PAID EMPLOYEE PER DEPARTMENT ==========")


highest_paid = result_row_number.filter(
    col("row_number") == 1
)


highest_paid.show()


# ============================================================
# 12. Stop Spark Session
# ============================================================

spark.stop()