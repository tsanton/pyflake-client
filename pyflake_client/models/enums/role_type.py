from enum import Enum


class RoleType(Enum):
    """snowflake role types"""
    ROLE = "ROLE"
    DATABASE_ROLE = "DATABASE_ROLE"
    
    def __str__(self) -> str:
        if self == RoleType.DATABASE_ROLE:
            return "DATABASE ROLE"

        return self.value


    def singularize(self) -> str:
        """singularize"""
        return str(self)

    def pluralize(self) -> str:
        """pluralize"""
        # if self == ObjectType.TABLE: // If we have an object that can't be pluralized (i.e. MASKING POLICY -> MASKING POILICIES)
        #     return "TABLES"
        return f"{str(self)}S"