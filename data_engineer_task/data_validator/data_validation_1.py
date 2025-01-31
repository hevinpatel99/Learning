import re
from datetime import date
from typing import Optional
from pydantic import BaseModel, condecimal, field_validator


class SchemaModel(BaseModel):
    PID: int  # Unique patient ID
    GESTAGE: Optional[condecimal(gt=0)] = None  # Gestational age, must be > 0
    DOB: date  # Date of birth
    YOB: int  # Year of birth
    PCODE: str  # Postal code
    CELL: Optional[str] = None  # Optional cell phone number
    HOME: Optional[str] = None  # Optional home phone number
    RACE: Optional[str] = None  # Race
    GENDER: str  # Gender
    CITY: Optional[str] = None  # City
    APARTMENT: Optional[str] = None  # Apartment
    STREET: Optional[str] = None  # Street
    REGION: Optional[str] = None  # Region
    INITIAL: Optional[str] = None  # Initials
    COUNTRY: Optional[str] = None  # Country
    EMAIL: Optional[str] = None  # Email address
    LNAME: str  # Last name
    FNAME: str  # First name

    # Validate cell and home phone number formats (e.g., 123-456-7890)
    @field_validator('CELL', 'HOME')
    def validate_phone(cls, v):
        if v is not None:
            if not re.match(r'^\d{3}-\d{3}-\d{4}$', v):
                raise ValueError('Phone number must be in the format 123-456-7890')
        return v

    # Validate gender to accept only 'Male' or 'Female'
    @field_validator('GENDER')
    def validate_gender(cls, v):
        if v not in ['Male', 'Female']:
            raise ValueError('Gender must be one of the following: Male, Female')
        return v
