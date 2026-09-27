from pyspark.sql import SparkSession


if __name__ == "__main__":

    # --------------------------------------------------
    # 1. Create Spark Session
    # --------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("PySpark Join Demo")
        .master("local[*]")
        .getOrCreate()
    )

    # --------------------------------------------------
    # 2. Employee Data
    # --------------------------------------------------

    employee_data = [
        (1, "Ravi", 10),
        (2, "Amit", 20),
        (3, "John", 30),
        (4, "Priya", 40)
    ]

    employee_df = spark.createDataFrame(
        employee_data,
        ["emp_id", "name", "dept_id"]
    )

    # --------------------------------------------------
    # 3. Department Data
    # --------------------------------------------------

    department_data = [
        (10, "IT"),
        (20, "HR"),
        (30, "Finance"),
        (50, "Sales")
    ]

    department_df = spark.createDataFrame(
        department_data,
        ["dept_id", "department"]
    )

    # --------------------------------------------------
    # 4. Display Employee Data
    # --------------------------------------------------

    print("\n========== EMPLOYEE DATA ==========")

    employee_df.show()

    # --------------------------------------------------
    # 5. Display Department Data
    # --------------------------------------------------

    print("\n========== DEPARTMENT DATA ==========")

    department_df.show()

    # --------------------------------------------------
    # 6. INNER JOIN
    # --------------------------------------------------

    print("\n========== INNER JOIN ==========")

    inner_df = employee_df.join(
        department_df,
        on="dept_id",
        how="inner"
    )

    inner_df.show()

    # --------------------------------------------------
    # 7. LEFT JOIN
    # --------------------------------------------------

    print("\n========== LEFT JOIN ==========")

    left_df = employee_df.join(
        department_df,
        on="dept_id",
        how="left"
    )

    left_df.show()

    # --------------------------------------------------
    # 8. RIGHT JOIN
    # --------------------------------------------------

    print("\n========== RIGHT JOIN ==========")

    right_df = employee_df.join(
        department_df,
        on="dept_id",
        how="right"
    )

    right_df.show()

    # --------------------------------------------------
    # 9. FULL OUTER JOIN
    # --------------------------------------------------

    print("\n========== FULL OUTER JOIN ==========")

    full_df = employee_df.join(
        department_df,
        on="dept_id",
        how="full"
    )

    full_df.show()

    # --------------------------------------------------
    # 10. LEFT SEMI JOIN
    # --------------------------------------------------

    print("\n========== LEFT SEMI JOIN ==========")

    semi_df = employee_df.join(
        department_df,
        on="dept_id",
        how="left_semi"
    )

    semi_df.show()

    # --------------------------------------------------
    # 11. LEFT ANTI JOIN
    # --------------------------------------------------

    print("\n========== LEFT ANTI JOIN ==========")

    anti_df = employee_df.join(
        department_df,
        on="dept_id",
        how="left_anti"
    )

    anti_df.show()

    input("Press Enter to stop Spark...")

    # --------------------------------------------------
    # 12. Stop Spark
    # --------------------------------------------------

    spark.stop()