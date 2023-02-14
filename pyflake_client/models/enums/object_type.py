"""object_type"""
from enum import Enum


class ObjectType(str, Enum):
    """ObjectType"""
    TABLE = "TABLE"
    VIEW = "VIEW"
    MATVIEW = "MATVIEW"
    ACCOUNT = "ACCOUNT"
    STAGE = "STAGE"
    FILE_FORMAT = "FILE_FORMAT"
    STREAM = "STREAM"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    SEQUENCE = "SEQUENCE"
    TASK = "TASK"

    def __str__(self):
        # Not using match self.type: for python 3.8 compatability
        if self == ObjectType.TABLE:
            return "TABLE"
        elif self == ObjectType.VIEW:
            return "VIEW"
        elif self == ObjectType.MATVIEW:
            return "MATERIALIZED VIEW"
        elif self == ObjectType.ACCOUNT:
            return "ACCOUNT"
        elif self == ObjectType.STAGE:
            return "STAGE"
        elif self == ObjectType.FILE_FORMAT:
            return "FILE FORMAT"
        elif self == ObjectType.STREAM:
            return "STREAM"
        elif self == ObjectType.PROCEDURE:
            return "PROCEDURE"
        elif self == ObjectType.FUNCTION:
            return "FUNCTION"
        elif self == ObjectType.SEQUENCE:
            return "SEQUENCE"
        elif self == ObjectType.TASK:
            return "TASK"
        else:
            return ""

    def singularize(self) -> str:
        """singularize"""
        return str(self)

    def pluralize(self) -> str:
        """pluralize"""
        if self == ObjectType.TABLE:
            return "TABLES"
        elif self == ObjectType.VIEW:
            return "VIEWS"
        elif self == ObjectType.MATVIEW:
            return "MATERIALIZED VIEWS"
        elif self == ObjectType.ACCOUNT:
            return "ACCOUNTS"
        elif self == ObjectType.STAGE:
            return "STAGES"
        elif self == ObjectType.FILE_FORMAT:
            return "FILE FORMATS"
        elif self == ObjectType.STREAM:
            return "STREAMS"
        elif self == ObjectType.PROCEDURE:
            return "PROCEDURES"
        elif self == ObjectType.FUNCTION:
            return "FUNCTIONS"
        elif self == ObjectType.SEQUENCE:
            return "SEQUENCES"
        elif self == ObjectType.TASK:
            return "TASKS"
