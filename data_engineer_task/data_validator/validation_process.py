import os

import pandas as pd

from data_engineer_task.config.file_validation_config import FILE_CONFIG  # Import the file configuration
from data_engineer_task.kafka_streaming_pipeline.utils import setup_logger

logger = setup_logger("DataValidation",
                      "/home/dev1070/Hevin_1070/hevin.softvan@gmail.com/projects/Python_Workspace/Learning/data_engineer_task/logs_files/file_validation.log")  # Create a logger for this module


class DataValidation:
    """
    A class responsible for validating data.
    """

    def __init__(self, data_frame, file_name):
        # Initialize the object with DataFrame and file name
        self.data_frame = data_frame
        self.file_name = file_name

    def validate_data(self):
        """
        Validate the data dynamically based on file name.
        Returns True if the data is valid, False otherwise.
        """

        name = os.path.splitext(self.file_name)[0]

        # Check if file_name is in the configuration
        if name not in FILE_CONFIG:
            logger.warning(f"Data validation skipped for unsupported file: {self.file_name}")
            return False

        config = FILE_CONFIG[name]
        required_columns = config['REQUIRED_COLUMNS']
        default_values = config['DEFAULT_VALUES']
        expected_data_types = config['EXPECTED_DATA_TYPES']

        logger.info(f"FILE_COLUMNS ---- {self.data_frame.columns}")

        # validate required columns
        if not self._validate_required_columns(required_columns):
            return False

        if not self._handle_null_values(required_columns, default_values):
            return False

        # # Validate email format if 'EMAIL' column is present
        # if 'EMAIL' in required_columns and not self._validate_email_format():
        #     return False

        # Validate the data types for columns
        if not self._validate_data_types(expected_data_types):
            return False

        # date_columns = ['DOB', 'START_DATE', 'END_DATE', 'DATE', 'DATE_OF_BIRTH', 'DATE_OF_DEATH', 'TIMESTAMP']
        # date_columns_to_convert = [col for col in date_columns if col in required_columns]

        # if date_columns_to_convert:
        #     for col in date_columns_to_convert:
        #         if not self._formate_date(col):
        #             logger.error(f"{col} conversion failed. Invalid data encountered.")
        #             return False

        return True

        # if 'DOB' in required_columns:
        #     # Attempt to convert the DOB column
        #     if not self._formate_date():
        #         logger.error("DOB conversion failed. Invalid data encountered.")
        #         return False
        #
        # if 'START_DATE' in required_columns:
        #     # Attempt to convert the START_DATE column
        #     if not self._formate_date():
        #         logger.error("START_DATE conversion failed. Invalid data encountered.")
        #         return False
        #
        # if 'END_DATE' in required_columns:
        #     # Attempt to convert the END_DATE column
        #     if not self._formate_date():
        #         logger.error("END_DATE conversion failed. Invalid data encountered.")
        #         return False

    def _validate_required_columns(self, required_columns):
        missing_columns = [col for col in required_columns if col not in self.data_frame.columns]
        if missing_columns:
            logger.error(f"Missing required columns: {', '.join(missing_columns)}")
            return False
        return True

    def _handle_null_values(self, required_columns, default_values):
        logger.info(f"Handling null values for columns: {required_columns}")
        """Handle missing values by filling them with default values."""
        for col in required_columns:
            if col in self.data_frame.columns:
                # Replace empty strings with NaN (pd.NA)
                self.data_frame[col] = self.data_frame[col].replace('', pd.NA)

                # Fill NaN values with the default value from default_values dictionary
                self.data_frame[col] = self.data_frame[col].fillna(default_values.get(col, 'Unknown'))
            else:
                logger.error(f"Column {col} is missing in the DataFrame.")
                return False  # Return False if a required column is missing
        return True  # Return True if null values were handled successfully

    def _validate_email_format(self):
        """Validate the format of the email address."""
        if 'EMAIL' not in self.data_frame.columns:
            logger.error("EMAIL column is missing.")
            return False

        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        invalid_emails = self.data_frame[~self.data_frame['EMAIL'].str.match(email_pattern, na=False)]

        if not invalid_emails.empty:
            logger.error(f"Invalid email format found in rows: {invalid_emails.index.tolist()}")
            return False
        return True

    def _validate_data_types(self, expected_data_types):
        """Validate the data types for each column dynamically."""
        for col, expected_type in expected_data_types.items():
            if col in self.data_frame.columns:
                # Try to convert the column to the expected type
                try:
                    if expected_type == 'float64':
                        self.data_frame[col] = pd.to_numeric(self.data_frame[col], errors='coerce').astype('float64')
                        self.data_frame[col] = self.data_frame[col].fillna(0.0)
                    elif expected_type == 'int64':
                        self.data_frame[col] = pd.to_numeric(self.data_frame[col], errors='coerce').astype(
                            'int64')  # for nullable integers
                        self.data_frame[col] = self.data_frame[col].fillna(0)

                    # elif expected_type in ['datetime64[ns]', 'datetime64[D]']:
                    #     # Print sample of current values for debugging
                    #     logger.info(
                    #         f"Sample values for '{col}' before conversion: {self.data_frame[col].head().tolist()}")
                    #
                    #     # Convert to datetime safely without chaining
                    #     self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors='coerce')
                    #
                    #     # Replace NaT with a default value
                    #     # Replace NaT with the desired default value (adjustable)
                    #     # default_date = pd.NaT  # or pd.NaT if no default date
                    #     self.data_frame[col] = self.data_frame[col].fillna('NaT')
                    #
                    #     # Check final dtype and log results
                    #     logger.info(
                    #         f"Sample values for '{col}' after conversion: {self.data_frame[col].head().tolist()}")

                    elif expected_type in ['datetime64[ns]', 'datetime64[D]']:
                        logger.info(
                            f"Sample values for '{col}' before conversion: {self.data_frame[col].head().tolist()}")

                        # Safely handle datetime parsing including timezone-aware timestamps
                        def ensure_datetime_ns(value):
                            if pd.isna(value):
                                return pd.NaT

                            try:
                                # Normalize Zulu time (UTC) by removing 'Z'
                                if isinstance(value, str) and value.endswith('Z'):
                                    value = value[:-1]

                                return pd.to_datetime(value, errors='coerce', utc=True).tz_localize(None)

                            except Exception:
                                return pd.NaT

                        # Apply conversion to handle mixed formats
                        self.data_frame[col] = self.data_frame[col].apply(ensure_datetime_ns)

                        # Check if there are still invalid dates
                        if self.data_frame[col].isnull().any():
                            logger.warning(f"Invalid dates found and converted to NaT in column '{col}'.")

                        # Ensure the correct datetime64 dtype
                        self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors='coerce')

                        logger.info(
                            f"Sample values for '{col}' after conversion: {self.data_frame[col].head().tolist()}")

                    # Add other type conversions as needed
                except Exception as e:
                    logger.error(f"Error converting column '{col}' to {expected_type}: {e}")
                    return False

                # Check if the type matches
                actual_type = str(self.data_frame[col].dtype)
                if actual_type != expected_type:
                    logger.error(
                        f"Column '{col}' has incorrect data type. Expected: {expected_type}, Found: {actual_type}")
                    return False
        return True

    # def _formate_date(self, col):
    #     """Dynamically handle date columns and format them to dd-mm-yyyy."""
    #     if col not in self.data_frame.columns:
    #         logger.error(f"Column '{col}' is missing in the DataFrame.")
    #         return False
    #
    #     logger.info(f"Attempting to convert '{col}' column to datetime...")
    #
    #     # Convert the column to datetime, invalid entries are set to NaT
    #     self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors='coerce')
    #     # self.data_frame[col] = pd.to_datetime(self.data_frame[col], format="ISO8601", errors='coerce')
    #
    #     # Check if there are invalid dates (NaT) and log a warning
    #     if self.data_frame[col].isnull().any():
    #         logger.warning(f"Invalid dates found and converted to NaT in column '{col}'.")
    #
    #     self.data_frame[col] = self.data_frame[col].fillna(pd.Timestamp("1900-01-01"))
    #
    #     # Format valid dates to "YYYY-MM-DD"
    #     self.data_frame[col] = self.data_frame[col].dt.strftime("%Y-%m-%d")
    #
    #     self.data_frame[col] = self.data_frame[col].astype(str)
    #
    #     logger.info(f"Formatted '{col}' to 'dd-mm-yyyy'.")
    #     return True
