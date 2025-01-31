FILE_CONFIG = {
    'patients.txt': {
        'REQUIRED_COLUMNS': ['GESTAGE', 'CITY', 'APARTMENT', 'STREET', 'CELL', 'DOB', 'REGION', 'INITIAL', 'PID',
                             'YOB', 'PCODE', 'RACE', 'GENDER', 'HOME', 'COUNTRY', 'EMAIL', 'LNAME', 'FNAME'],
        'DEFAULT_VALUES': {
            'GESTAGE': 0.0, 'CITY': 'Unknown', 'APARTMENT': 'N/A', 'STREET': 'N/A', 'CELL': 'N/A',
            'DOB': 'NaT', 'REGION': 'Unknown', 'INITIAL': 'N/A', 'PID': 0, 'YOB': 0,
            'PCODE': '00000', 'RACE': 'Unknown', 'GENDER': 'Unknown', 'HOME': 'N/A', 'COUNTRY': 'Unknown',
            'EMAIL': 'no-reply@example.com', 'LNAME': 'Unknown', 'FNAME': 'Unknown',
        },
        'EXPECTED_DATA_TYPES': {
            'GESTAGE': 'float64', 'CITY': 'object', 'APARTMENT': 'object', 'STREET': 'object', 'CELL': 'object',
            'DOB': 'datetime64[ns]', 'REGION': 'object', 'INITIAL': 'object', 'PID': 'object', 'YOB': 'int64',
            'PCODE': 'object', 'RACE': 'object', 'GENDER': 'object', 'HOME': 'object', 'COUNTRY': 'object',
            'EMAIL': 'object', 'LNAME': 'object', 'FNAME': 'object',
        }
    }, 'allergies.txt': {
        'REQUIRED_COLUMNS': ['PID', 'STATEMENT', 'TYPE', 'ALLERGEN', 'SYSTEM', 'CODE', 'START_DATE', 'END_DATE',
                             'REACTION', 'SNOMED', 'SEVERITY'],
        'DEFAULT_VALUES': {
            'PID': 0,
            'STATEMENT': 'unknown',
            'TYPE': 'unknown',
            'ALLERGEN': 'No known allergies',
            'SYSTEM': 'Unknown system',
            'CODE': 'Unknown',
            'START_DATE': 'NaT',  # Default start date
            'END_DATE': 'NaT',  # End date may be optional
            'REACTION': 'Unknown reaction',
            'SNOMED': 'Unknown',
            'SEVERITY': 'unknown'
        },
        'EXPECTED_DATA_TYPES': {
            'PID': 'int64',
            'STATEMENT': 'object',
            'TYPE': 'object',
            'ALLERGEN': 'object',
            'SYSTEM': 'object',
            'CODE': 'object',
            'START_DATE': 'datetime64[ns]',
            'END_DATE': 'datetime64[ns]',  # Optional, could be nullable
            'REACTION': 'object',
            'SNOMED': 'object',  # If the SNOMED code is numeric
            'SEVERITY': 'object'
        }
    },
    'labs.txt': {
        'REQUIRED_COLUMNS': ['PID', 'DATE', 'LOINC', 'SCALE', 'NAME', 'VALUE', 'LOW', 'HIGH', 'UNITS'],
        'DEFAULT_VALUES': {
            'PID': 0,
            'DATE': 'NaT',  # Default date if not provided
            'LOINC': 'Unknown',
            'SCALE': 'Qn',
            'NAME': 'Unknown test',
            'VALUE': 0,  # Default value for VALUE
            'LOW': 0,  # Default low value
            'HIGH': 0,  # Default high value
            'UNITS': 'N/A'
        },
        'EXPECTED_DATA_TYPES': {
            'PID': 'int64',
            'DATE': 'datetime64[ns]',
            'LOINC': 'object',
            'SCALE': 'object',
            'NAME': 'object',
            'VALUE': 'float64',
            'LOW': 'float64',
            'HIGH': 'float64',
            'UNITS': 'object'
        }
    },
    'meds.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'START_DATE', 'END_DATE', 'RXNORM', 'NAME', 'SIG', 'Q', 'DAYS', 'REFILLS',
                             'Q_TO_TAKE_VALUE', 'Q_TO_TAKE_UNIT', 'FREQUENCY_VALUE', 'FREQUENCY_UNIT'],
        'DEFAULT_VALUES': {
            'ID': 0,
            'PID': 0,
            'START_DATE': 'NaT',  # Default start date
            'END_DATE': 'NaT',  # End date may be optional
            'RxNorm': 0,
            'Name': 'Unknown',
            'SIG': 'Unknown',
            'Q': 0,
            'DAYS': 0,
            'REFILLS': 0,
            'Q_TO_TAKE_VALUE': 0,
            'Q_TO_TAKE_UNIT': 'Unknown',
            'FREQUENCY_VALUE': 0,
            'FREQUENCY_UNIT': 'Unknown'
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64',
            'PID': 'int64',
            'START_DATE': 'datetime64[ns]',
            'END_DATE': 'datetime64[ns]',  # Optional, could be nullable
            'RXNORM': 'int64',
            'name': 'object',
            'SIG': 'object',
            'Q': 'int64',
            'DAYS': 'int64',
            'REFILLS': 'int64',
            'Q_TO_TAKE_VALUE': 'int64',
            'Q_TO_TAKE_UNIT': 'object',
            'FREQUENCY_VALUE': 'int64',
            'FREQUENCY_UNIT': 'object'
        }
    },
    'familyhistory.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'RELATIVE_CODE', 'RELATIVE_TITLE', 'DATE_OF_BIRTH',
                             'DATE_OF_DEATH',
                             'PROBLEM_CODE', 'PROBLEM_TITLE', 'HEIGHT_CM'],
        'DEFAULT_VALUES': {
            'ID': 0, 'PID': 0, 'RELATIVE_CODE': 'N/A', 'RELATIVE_TITLE': 'Unknown', 'DATE_OF_BIRTH': 'NaT',
            'DATE_OF_DEATH': 'NaT', 'PROBLEM_CODE': 'Unknown', 'PROBLEM_TITLE': 'Unknown', 'HEIGHT_CM': 0.0,
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64', 'PID': 'int64', 'RELATIVE_CODE': 'object', 'RELATIVE_TITLE': 'object',
            'DATE_OF_BIRTH': 'datetime64[ns]', 'DATE_OF_DEATH': 'datetime64[ns]', 'PROBLEM_CODE': 'object',
            'PROBLEM_TITLE': 'object', 'HEIGHT_CM': 'float64',
        }
    },
    'problems.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'START_DATE', 'END_DATE', 'SNOMED', 'NAME'],
        'DEFAULT_VALUES': {
            'ID': 0, 'PID': 0, 'START_DATE': 'NaT', 'END_DATE': 'NaT',
            'SNOMED': 'Unknown', 'NAME': 'Unknown'
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64', 'PID': 'int64', 'START_DATE': 'datetime64[ns]',
            'END_DATE': 'datetime64[ns]', 'SNOMED': 'object', 'NAME': 'object'
        }
    },
    'procedures.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'DATE', 'SNOMED', 'NAME', 'NOTES'],
        'DEFAULT_VALUES': {
            'ID': 0, 'PID': 0, 'DATE': 'NaT',
            'SNOMED': 'Unknown', 'NAME': 'Unknown', 'NOTES': 'No notes'
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64', 'PID': 'int64', 'DATE': 'datetime64[ns]'
            , 'SNOMED': 'object', 'NAME': 'object', 'NOTES': 'object'
        }
    },
    'refills.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'DATE', 'RXN', 'DAYS', 'Q'],
        'DEFAULT_VALUES': {
            'ID': 0, 'PID': 0, 'DATE': 'NaT',
            'RXN': 0, 'DAYS': 0, 'Q': 'No notes'
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64', 'PID': 'int64', 'DATE': 'datetime64[ns]',
            'RXN': 'int64', 'DAYS': 'int64', 'Q': 'object'
        }
    },
    'socialhistory.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'SMOKINGSTATUSCODE'],
        'DEFAULT_VALUES': {
            'ID': 0, 'PID': 0, 'SMOKINGSTATUSCODE': 'Unknown'

        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64', 'PID': 'int64', 'SMOKINGSTATUSCODE': 'object'
        }
    },
    'vitals.txt': {
        'REQUIRED_COLUMNS': ['ID', 'PID', 'TIMESTAMP', 'START_DATE', 'END_DATE', 'ENCOUNTER_TYPE', 'HEART_RATE',
                             'RESPIRATORY_RATE', 'TEMPERATURE', 'WEIGHT', 'HEIGHT', 'BMI', 'SYSTOLIC', 'DIASTOLIC',
                             'OXYGEN_SATURATION', 'HEAD_CIRCUMFERENCE', 'BP_SITE', 'BP_METHOD', 'BP_POSITION'],
        'DEFAULT_VALUES': {
            'ID': 0,
            'PID': 0,
            'TIMESTAMP': 'NaT',
            'START_DATE': 'NaT',
            'END_DATE': 'NaT',
            'ENCOUNTER_TYPE': 'N/A',
            'HEART_RATE': 0.0,
            'RESPIRATORY_RATE': 0.0,
            'TEMPERATURE': 0.0,
            'WEIGHT': 0.0,
            'HEIGHT': 0.0,
            'BMI': 0.0,
            'SYSTOLIC': 0.0,
            'DIASTOLIC': 0.0,
            'OXYGEN_SATURATION': 0.0,
            'HEAD_CIRCUMFERENCE': 0.0,
            'BP_SITE': 'N/A',
            'BP_METHOD': 'N/A',
            'BP_POSITION': 'N/A'
        },
        'EXPECTED_DATA_TYPES': {
            'ID': 'int64',
            'PID': 'int64',
            'TIMESTAMP': 'datetime64[ns]',
            'START_DATE': 'datetime64[ns]',
            'END_DATE': 'datetime64[ns]',
            'ENCOUNTER_TYPE': 'object',
            'HEART_RATE': 'float64',
            'RESPIRATORY_RATE': 'float64',
            'TEMPERATURE': 'float64',
            'WEIGHT': 'float64',
            'HEIGHT': 'float64',
            'BMI': 'float64',
            'SYSTOLIC': 'float64',
            'DIASTOLIC': 'float64',
            'OXYGEN_SATURATION': 'float64',
            'HEAD_CIRCUMFERENCE': 'float64',
            'BP_SITE': 'object',
            'BP_METHOD': 'object',
            'BP_POSITION': 'object'
        }
    }

}
