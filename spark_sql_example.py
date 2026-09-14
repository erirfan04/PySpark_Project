from pyspark.sql import SparkSession

# 1. Create SparkSession
spark = (
    SparkSession.builder
    .appName("SparkSQLDemo")
    .master("local[*]")
    .getOrCreate()
)

# 2. Read CSV file
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/employees.csv")
)

print("Original Employee Data:")
df.show()

# 3. Register DataFrame as a temporary SQL view
df.createOrReplaceTempView("employees")

# 4. Execute Spark SQL query
result = spark.sql("""
    SELECT name, department, salary
    FROM employees
    WHERE salary > 60000
""")

print("Employees with salary greater than 60000:")
result.show()

# 5. GroupBy using Spark SQL
department_salary = spark.sql("""
    SELECT
        department,
        COUNT(*) AS employee_count,
        AVG(salary) AS average_salary
    FROM employees
    GROUP BY department
    ORDER BY average_salary DESC
""")

print("Department-wise Salary Analysis:")
department_salary.show()

input("Press Enter to stop the Spark application...")
# 6. Stop Spark
spark.stop()