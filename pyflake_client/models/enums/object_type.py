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
        match self:
            case ObjectType.TABLE:
                return "TABLE"
            case ObjectType.VIEW:
                return "VIEW"
            case ObjectType.MATVIEW:
                return "MATERIALIZED VIEW"
            case ObjectType.ACCOUNT:
                return "ACCOUNT"
            case ObjectType.STAGE:
                return "STAGE"
            case ObjectType.FILE_FORMAT:
                return "FILE FORMAT"
            case ObjectType.STREAM:
                return "STREAM"
            case ObjectType.PROCEDURE:
                return "PROCEDURE"
            case ObjectType.FUNCTION:
                return "FUNCTION"
            case ObjectType.SEQUENCE:
                return "SEQUENCE"
            case ObjectType.TASK:
                return "TASK"
            case _:
                pass  # __str__ doesn't accept default case as return
        return ""

    def singularize(self) -> str:
        """singularize"""
        return str(self)

    def pluralize(self) -> str:
        """pluralize"""
        match self:
            case ObjectType.TABLE:
                return "TABLES"
            case ObjectType.VIEW:
                return "VIEWS"
            case ObjectType.MATVIEW:
                return "MATERIALIZED VIEWS"
            case ObjectType.ACCOUNT:
                return "ACCOUNTS"
            case ObjectType.STAGE:
                return "STAGES"
            case ObjectType.FILE_FORMAT:
                return "FILE FORMATS"
            case ObjectType.STREAM:
                return "STREAMS"
            case ObjectType.PROCEDURE:
                return "PROCEDURES"
            case ObjectType.FUNCTION:
                return "FUNCTIONS"
            case ObjectType.SEQUENCE:
                return "SEQUENCES"
            case ObjectType.TASK:
                return "TASKS"
