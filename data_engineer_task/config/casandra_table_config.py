TABLE_CONFIG = {
    "patient_allergies": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "STATEMENT": "TEXT",
        "TYPE": "TEXT",
        "ALLERGEN": "TEXT",
        "SYSTEM": "TEXT",
        "CODE": "TEXT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "REACTION": "TEXT",
        "SNOMED": "INT",
        "SEVERITY": "TEXT"
    },
    "patient_info": {
        "PID": "INT PRIMARY KEY",
        "GESTAGE": "FLOAT",
        "CITY": "TEXT",
        "APARTMENT": "TEXT",
        "STREET": "TEXT",
        "CELL": "TEXT",
        "DOB": "DATE",
        "GENDER": "TEXT",
        "REGION": "TEXT",
        "INITIAL": "TEXT",
        "YOB": "INT",
        "PCODE": "INT",
        "RACE": "TEXT",
        "HOME": "TEXT",
        "COUNTRY": "TEXT",
        "EMAIL": "TEXT",
        "LNAME": "TEXT",
        "FNAME": "TEXT"
    },
    "patient_diagnoses": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "SNOMED": "INT",
        "NAME": "TEXT"
    },
    "patient_procedure": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "SNOMED": "INT",
        "NAME": "TEXT",
        "NOTES": "TEXT"
    },
    "patient_refills": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "RXN": "INT",
        "DAYS": "INT",
        "QUANTITY": "INT"
    },
    "patient_labs": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "LOINC": "TEXT",
        "SCALE": "TEXT",
        "NAME": "TEXT",
        "VALUE": "DOUBLE",
        "LOW": "DOUBLE",
        "HIGH": "DOUBLE",
        "UNITS": "TEXT"
    },
    "patient_meds": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "RXNORM": "INT",
        "NAME": "TEXT",
        "SIG": "TEXT",
        "QUANTITY": "INT",
        "DAYS": "INT",
        "REFILLS": "INT",
        "Q_TO_TAKE_VALUE": "INT",
        "Q_TO_TAKE_UNIT": "TEXT",
        "FREQUENCY_VALUE": "INT",
        "FREQUENCY_UNIT": "TEXT"
    },
    "patient_family_history": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "RELATIVE_CODE": "INT",
        "RELATIVE_TITLE": "TEXT",
        "DATE_OF_BIRTH": "DATE",
        "DATE_OF_DEATH": "DATE",
        "PROBLEM_TITLE": "TEXT",
        "PROBLEM_CODE": "TEXT",
        "HEIGHT_CM": "DOUBLE"
    },
    "patient_vitals": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "DATE": "DATE",
        "START_DATE": "DATE",
        "END_DATE": "DATE",
        "ENCOUNTER_TYPE": "TEXT",
        "HEART_RATE": "INT",
        "RESPIRATORY_RATE": "INT",
        "TEMPERATURE": "DOUBLE",
        "WEIGHT": "DOUBLE",
        "HEIGHT": "DOUBLE",
        "BMI": "DOUBLE",
        "SYSTOLIC": "INT",
        "DIASTOLIC": "INT",
        "OXYGEN_SATURATION": "DOUBLE",
        "HEAD_CIRCUMFERENCE": "DOUBLE",
        "BP_SITE": "TEXT",
        "BP_METHOD": "TEXT",
        "BP_POSITION": "TEXT"
    },
    "patient_social_history": {
        "ID": "INT PRIMARY KEY",
        "PID": "INT",
        "SMOKINGSTATUSCODE": "BIGINT"
    }
}
