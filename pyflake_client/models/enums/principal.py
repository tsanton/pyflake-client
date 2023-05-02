"""object_type"""
from enum import Enum

class Principal(str, Enum):
    """Principal"""
    USER = "TABLE"
    DATABASE_ROLE = "DATABASE_ROLE"
    ROLE = "ROLE"

    def __str__(self):
        if self == Principal.DATABASE_ROLE:
            return "DATABASE ROLE"
        else:
            return self

    def snowflake_type(self) -> str:
        """snowflake_type returns 'ROLE', 'DATABASE ROLE' or 'USER'"""
        return str(self)

    def grant_type(self) -> str:
        """snowflake_type returns 'ROLE', 'DATABASE_ROLE' or 'USER'"""
        if self == Principal.DATABASE_ROLE:
            return "DATABASE_ROLE"
        return str(self)