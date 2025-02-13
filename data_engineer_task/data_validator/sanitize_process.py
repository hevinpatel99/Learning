import os

import pandas as pd

from data_engineer_task.config.file_validation_config import FILE_CONFIG  # Import the file configuration
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger

logger = setup_logger("DataSanitization",
                      "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/file_validation.log")  # Create a logger for this module


class DataSanitization:
    """
    A class responsible for sanitizing the data.
    """

    def __init__(self, data_frame, file_name):
        # Initialize the object with DataFrame and file name
        self.data_frame = data_frame
        self.file_name = file_name

    def sanitize_data(self):
        logger.info(f"FILE_NAME ---- {self.file_name}")

        name = os.path.splitext(self.file_name)[0]

        print(f"file name: {name}")

        # Check if file_name is in the configuration
        if name not in FILE_CONFIG:
            logger.warning(f"Data sanitization skipped for unsupported file: {self.file_name}")
            return False

        config = FILE_CONFIG[name]
        print(f"config file : {config}")
        # required_columns = config['REQUIRED_COLUMNS']

        logger.info(f"FILE_COLUMNS ---- {self.data_frame.columns}")

        sanitization_status = self._sanitize_string_columns()

        if sanitization_status:
            logger.info(f"String column sanitization completed successfully for file: {self.file_name}")
        else:
            logger.info(
                f"No string columns found or no changes were needed in {self.file_name}. Skipping string sanitization.")

        return True

    # def _sanitize_string_columns(self):
    #
    #     if self.data_frame.empty:
    #         logger.warning("Data frame is empty. Skipping sanitization.")
    #         return False
    #
    #     """
    #     Trim whitespace and capitalize the first letter of each word for string columns.
    #     """
    #     for col in self.data_frame.select_dtypes(include=['object']).columns:
    #         # self.data_frame[col] = self.data_frame[col].astype(str).str.strip().str.title()  # Trim and capitalize
    #         logger.info(f"Processing column: {col}")
    #
    #         # Get original values before sanitization
    #         original_values = self.data_frame[col].dropna().unique()  # Unique non-null values
    #         logger.info(f"Original values in '{col}': {original_values}")
    #
    #         # Apply sanitization: Trim whitespace & capitalize first letter of each word
    #         self.data_frame[col] = self.data_frame[col].astype(str).str.strip().str.title()
    #
    #         # Get sanitized values after processing
    #         sanitized_values = self.data_frame[col].dropna().unique()
    #         logger.info(f"Sanitized values in '{col}': {sanitized_values}")
    #         logger.info(f"Sanitized column '{col}' - Trimmed whitespace & capitalized words.")

    def _sanitize_string_columns(self):
        """
        Trim whitespace and capitalize the first letter of each word for string columns.
        """
        if self.data_frame.empty:
            logger.warning("Data frame is empty. Skipping sanitization.")
            return False

        string_columns = self.data_frame.select_dtypes(include=['object']).columns

        if not string_columns.any():  # No string columns found
            logger.info("No string columns found in the DataFrame. Skipping sanitization.")
            return False

        modified = False  # Track if any changes are made

        for col in string_columns:
            original_values = self.data_frame[col].dropna().unique()

            if col == "EMAIL":
                sanitized_values = self.data_frame[col].astype(str).str.strip().str.lower()
            else:
                sanitized_values = self.data_frame[col].astype(str).str.strip().str.title()

            if not sanitized_values.equals(self.data_frame[col]):  # Check if sanitization changed any value
                modified = True
                self.data_frame[col] = sanitized_values

            logger.info(f"Processing column: {col}")
            logger.info(f"Original values in '{col}': {original_values}")
            logger.info(f"Sanitized values in '{col}': {self.data_frame[col].dropna().unique()}")
            logger.info(f"Sanitized column '{col}' - Trimmed whitespace & capitalized words.")

        return modified
