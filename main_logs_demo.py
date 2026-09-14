import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from lib import logger_config
import lib


# --------------------------------------------------
# STEP 1: Create application logger
# --------------------------------------------------

logger = logging.getLogger(__name__)


# --------------------------------------------------
# STEP 2: Start the application
# --------------------------------------------------

if __name__ == "__main__":

    # Configure logging using logger_config.py
    logger_config.create_logger()

    logger.info("Application started")

    # Initialize SparkSession
    spark = None

    try:

        spark = (
            SparkSession.builder
            .appName("EmployeeLoggingDemo")
            .master("local[2]")
            .getOrCreate()
        )

        # Reduce Spark's own console logging
        spark.sparkContext.setLogLevel("WARN")

        logger.info("SparkSession created successfully")

        # --------------------------------------------------
        # STEP 3: Read dummy CSV data
        # --------------------------------------------------

        logger.info("Reading employee CSV file")

        employees_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data/employees.csv")
        )

        logger.info("Employee CSV file loaded successfully")

        # --------------------------------------------------
        # STEP 4: Display input data
        # --------------------------------------------------

        print("\nOriginal Employee Data:")

        employees_df.show()

        logger.info("Displayed original employee data")

        # --------------------------------------------------
        # STEP 5: Apply transformation
        # --------------------------------------------------

        logger.info("Filtering employees from IT department")

        it_employees_df = employees_df.filter(
            col("department") == "IT"
        )

        logger.info("IT department filter applied")

        # --------------------------------------------------
        # STEP 6: Trigger Spark execution
        # --------------------------------------------------

        logger.info("Displaying filtered IT employees")

        it_employees_df.show()

        logger.info("Filtered employee data displayed successfully")

        # --------------------------------------------------
        # STEP 7: Count records
        # --------------------------------------------------

        employee_count = it_employees_df.count()

        logger.info(
            f"Number of IT employees: {employee_count}"
        )

        # --------------------------------------------------
        # STEP 8: Application completed
        # --------------------------------------------------

        logger.info("Application completed successfully")
        input("Press Enter to exit...")

    except Exception:

        logger.exception("Application failed with an exception")

        raise

    finally:

        if spark is not None:
            spark.stop()
            logger.info("SparkSession stopped")