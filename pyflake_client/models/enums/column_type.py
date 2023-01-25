"""column_type"""
from enum import Enum


class ColumnType(str, Enum):
    """ColumnType"""
    NUMBER = "NUMBER"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    VARCHAR = "VARCHAR"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"
    VARIANT = "VARIANT"
