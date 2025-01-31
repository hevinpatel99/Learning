import logging
import pandas as pd
from dateutil import parser

from file_config.file_validation_config import FILE_CONFIG  # Import the file configuration

logger = logging.getLogger('data_validation_process')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


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

        logger.info(f"FILE_NAME ---- {self.file_name}")
        # Check if file_name is in the configuration
        if self.file_name not in FILE_CONFIG:
            logger.warning(f"Data validation skipped for unsupported file: {self.file_name}")
            return False

        # Fetch file-specific config
        config = FILE_CONFIG[self.file_name]
        required_columns = config['REQUIRED_COLUMNS']
        default_values = config['DEFAULT_VALUES']
        expected_data_types = config['EXPECTED_DATA_TYPES']

        logger.info(f"FILE_COLUMNS ---- {self.data_frame.columns}")

        # validate required columns
        if not self._validate_required_columns(required_columns):
            return False
        self._handle_null_values(required_columns, default_values)

        # # Validate email format if 'EMAIL' column is present
        # if 'EMAIL' in required_columns and not self._validate_email_format():
        #     return False

        # Validate the data types for columns
        if not self._validate_data_types(expected_data_types):
            return False

        date_columns = ['DOB', 'START_DATE', 'END_DATE', 'DATE', 'DATE_OF_BIRTH', 'DATE_OF_DEATH', 'TIMESTAMP']
        date_columns_to_convert = [col for col in date_columns if col in required_columns]

        if date_columns_to_convert:
            for col in date_columns_to_convert:
                if not self._formate_date(col):
                    logger.error(f"{col} conversion failed. Invalid data encountered.")
                    return False

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
                    elif expected_type == 'datetime64[ns]':
                        # Print sample of current values for debugging
                        logger.info(
                            f"Sample values for '{col}' before conversion: {self.data_frame[col].head().tolist()}")

                        # Convert to datetime safely without chaining
                        self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors='coerce')

                        # Replace NaT with a default value
                        # Replace NaT with the desired default value (adjustable)
                        # default_date = pd.NaT  # or pd.NaT if no default date
                        self.data_frame[col] = self.data_frame[col].fillna('NaT')

                        # Check final dtype and log results
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

    # def _formate_date(self):
    #     logger.info(f"Before conversion - First few 'DOB' values: {self.data_frame['DOB'].head()}")
    #     print(f"{self.data_frame['DOB']}")
    #
    #     # Log rows with invalid DOB values before conversion
    #     invalid_dob_rows = self.data_frame[self.data_frame['DOB'].isna()]
    #     if not invalid_dob_rows.empty:
    #         logger.warning(f"Rows with invalid 'DOB' values before conversion: {invalid_dob_rows}")
    #
    #     # Attempt to convert the 'DOB' column to datetime64[ns], invalid entries will be set to NaT
    #     self.data_frame['DOB'] = pd.to_datetime(self.data_frame['DOB'], errors='coerce')
    #
    #     # Log the result after conversion
    #     logger.info(f"After conversion - First few 'DOB' values: {self.data_frame['DOB'].head()}")
    #
    #     # Check if any values are invalid (NaT)
    #     if self.data_frame['DOB'].isnull().any():
    #         logger.warning("Some values in 'DOB' column could not be converted and are now NaT.")
    #         return False
    #
    #     # Format the 'DOB' column to 'dd-mm-yyyy' after checking for NaT
    #     self.data_frame['DOB'] = self.data_frame['DOB'].dt.strftime('%d-%m-%Y')
    #
    #     logger.info("Converted 'DOB' column to datetime64[ns] and formatted it.")
    #     return True

    def _formate_date(self, col):

        print(f"DATE TIME HANDLING --- {self.data_frame[col]}")
        """Dynamically handle date columns and format them."""
        if col not in self.data_frame.columns:
            logger.error(f"Column '{col}' is missing in the DataFrame.")
            return False

        logger.info(f"Attempting to convert '{col}' column to datetime...")

        # Attempt to convert the column to datetime with invalid entries set to NaT
        self.data_frame[col] = pd.to_datetime(self.data_frame[col], errors='coerce')

        print(f"DATE TIME HANDLING 2222--- {self.data_frame[col]}")

        # Check if any invalid dates exist (NaT)
        if self.data_frame[col].isnull().any():
            logger.warning(f"Invalid dates found and converted to NaT in column '{col}'.")

        # Format valid date columns to 'dd-mm-yyyy'
        # if self.data_frame[col].dtype == 'datetime64[ns]':
        #     self.data_frame[col] = self.data_frame[col].dt.strftime('%d-%m-%Y')
        #     logger.info(f"Formatted '{col}' to 'dd-mm-yyyy'.")s

        if pd.api.types.is_datetime64_any_dtype(self.data_frame[col]):
            # Format dates, keeping NaT entries intact
            self.data_frame[col] = self.data_frame[col].apply(
                lambda x: x.strftime('%d-%m-%Y') if not pd.isna(x) or pd.notna(x) else pd.NaT
            )

            print(f"DATE TIME HANDLING 33333--- {self.data_frame[col]}")
            logger.info(f"Formatted '{col}' to 'dd-mm-yyyy'.")
        return True

    # def handle_date_columns(self):
    #     # Select columns with potential date-like data
    #     date_columns = []
    #
    #     # Detect columns that can be converted to datetime
    #     for col in self.data_frame.columns:
    #         try:
    #             pd.to_datetime(self.data_frame[col], errors='raise')
    #             date_columns.append(col)
    #         except (ValueError, TypeError):
    #             continue
    #
    #     # Process date columns if found
    #     for col in date_columns:
    #         try:
    #             self.data_frame[col] = pd.to_datetime(self.data_frame[col],
    #                                                   errors='coerce')  # Coerce invalid dates to NaT
    #             if self.data_frame[col].isna().any():
    #                 logger.warning(f"Invalid dates found in column '{col}'")
    #         except Exception as e:
    #             logger.error(f"Error processing column '{col}': {e}")
    #     return self.data_frame
