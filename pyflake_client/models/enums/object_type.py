"""object_type"""
from enum import Enum


class ObjectType(str, Enum):
    """ObjectType"""
    TABLE = "TABLE"
    VIEW = "VIEWS"
    MATVIEW = "MATVIEW"

    def __str__(self):
        match self:
            case ObjectType.TABLE:
                return "TABLE"
            case ObjectType.VIEW:
                return "VIEW"
            case ObjectType.MATVIEW:
                return "MATERIALIZED VIEW"
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
