import logging
import pandas as pd

logger = logging.getLogger('data_validation_process')


class DataValidation:
    """
    A class responsible for validating data.
    """

    def __init__(self, df, file_name):
        # Constructor to initialize the object with the input DataFrame and file name
        self.df = df  # Instance attribute
        self.file_name = file_name

    def validate_data(self):
        """
        Validate the data within the DataFrame.
        This method checks for missing values, incorrect formats, etc.
        Returns True if the data is valid, False otherwise.
        """

        if self.file_name == 'patients.txt':
            # Example 1: Check if any required columns are missing or empty
            required_columns = ["GESTAGE", "CITY", "APARTMENT", "STREET", "CELL", "DOB", "REGION", "INITIAL", "PID",
                                "YOB", "PCODE", "RACE", "GENDER", "HOME", "COUNTRY", "EMAIL", "LNAME", "FNAME"]

            # Check if required columns exist in the DataFrame
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {', '.join(missing_columns)}")
                return False

            self._handle_null_values(required_columns)

            # Optionally, you could validate specific columns (like 'EMAIL', 'DOB')
            if not self._validate_email_format():
                return False


            if not self._validate_column_type('GESTAGE', 'float64'):
                self.df['GESTAGE'] = pd.to_numeric(self.df['GESTAGE'], errors='coerce')
                logger.info("Converted 'GESTAGE' column to float64.")

            if not self._validate_column_type('DOB', 'datetime64[ns]'):
                """Convert DOB column to datetime64[ns]."""
                try:
                    # Before conversion, check and log the first few values in 'DOB' column
                    logger.info(f"Before conversion - First few 'DOB' values: {self.df['DOB'].head()}")
                    print(f"{self.df['DOB']}")
                    # Attempt to convert the 'DOB' column to datetime64[ns], invalid entries will be set to NaT
                    self.df['DOB'] = pd.to_datetime(self.df['DOB'], errors='coerce')

                    # Log the result after conversion
                    logger.info(f"After conversion - First few 'DOB' values: {self.df['DOB'].head()}")

                    self.df['DOB'] = self.df['DOB'].dt.strftime('%Y-%m-%d')

                    # Check if any values are invalid (NaT)
                    if self.df['DOB'].isnull().any():
                        logger.warning("Some values in 'DOB' column could not be converted and are now NaT.")
                        return False

                    logger.info("Converted 'DOB' column to datetime64[ns].")
                except Exception as e:
                    logger.error(f"Error converting 'DOB' column: {e}")
                    return False
                return True

            if not self._validate_column_type('YOB', 'int64'):
                self.df['YOB'] = pd.to_numeric(self.df['YOB'], errors='coerce')
                logger.info("Converted 'YOB' column to int64.")

            if not self._validate_data_types():
                return False

        else:
            logger.warning(f"Data validation skipped for unsupported file: {self.file_name}")

        return True

    def _handle_null_values(self, required_columns):
        """Check for null/empty values and set default values for non-required columns."""
        default_values = {
            'GESTAGE': 0.0,  # Default value for GESTAGE
            'CITY': 'Unknown',
            'APARTMENT': 'N/A',
            'STREET': 'N/A',
            'CELL': 'N/A',
            'DOB': '0000-00-00',
            'REGION': 'Unknown',
            'INITIAL': 'N/A',
            'PID': 'Unknown',
            'YOB': 0,  # Default year of birth
            'PCODE': '00000',
            'RACE': 'Unknown',
            'GENDER': 'Unknown',
            'HOME': 'N/A',
            'COUNTRY': 'Unknown',
            'EMAIL': 'no-reply@example.com',
            'LNAME': 'Unknown',
            'FNAME': 'Unknown',
        }

        # Only handle columns present in DataFrame
        for col in required_columns:
            if col in self.df.columns:
                self.df[col].replace('', pd.NA, inplace=True)
                self.df[col].fillna(default_values[col], inplace=True)
            else:
                logger.error(f"Column {col} is missing in the DataFrame.")

    # validate email
    def _validate_email_format(self):
        """Validate the format of the email address."""
        if 'EMAIL' not in self.df.columns:
            logger.error("EMAIL column is missing.")
            return False

        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        invalid_emails = self.df[~self.df['EMAIL'].str.match(email_pattern, na=False)]

        if not invalid_emails.empty:
            logger.error(f"Invalid email format found in rows: {invalid_emails.index.tolist()}")
            return False

        return True

    def _validate_data_types(self):
        """Check if the columns match the expected data types."""
        expected_data_types = {
            'GESTAGE': 'float64',
            'CITY': 'object',
            'APARTMENT': 'object',
            'STREET': 'object',
            'CELL': 'object',
            'DOB': 'datetime64[ns]',
            'REGION': 'object',
            'INITIAL': 'object',
            'PID': 'object',
            'YOB': 'int64',
            'PCODE': 'object',
            'RACE': 'object',
            'GENDER': 'object',
            'HOME': 'object',
            'COUNTRY': 'object',
            'EMAIL': 'object',
            'LNAME': 'object',
            'FNAME': 'object',
        }

        # Check if columns are of expected data types
        for col, expected_type in expected_data_types.items():
            if col in self.df.columns:
                actual_type = str(self.df[col].dtype)
                if actual_type != expected_type:
                    logger.error(
                        f"Column '{col}' has incorrect data type. Expected: {expected_type}, Found: {actual_type}")
                    return False
        return True

    def _validate_column_type(self, column, expected_dtype):
        """Validate the data typeof a column."""
        if column not in self.df.columns:
            logger.error(f"Column '{column}' is missing.")
            return False

        actual_dtype = self.df[column].dtype
        if actual_dtype != expected_dtype:
            logger.error(
                f"Column '{column}' has incorrect data type. Expected: {expected_dtype}, Found: {actual_dtype}")
            return False
        return True
