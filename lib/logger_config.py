import os
import logging


def create_logger():
    # Create logs folder
    os.makedirs("logs", exist_ok=True)

    # Configure application logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs/employee_app.log"),
            logging.StreamHandler()
        ],
        force=True
    )

    # Create logger for this module
    logger = logging.getLogger(__name__)

    return logger