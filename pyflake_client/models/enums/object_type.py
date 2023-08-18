# -*- coding: utf-8 -*-
from enum import Enum


class ObjectType(str, Enum):
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
        if self == ObjectType.MATVIEW:
            return "MATERIALIZED VIEW"
        elif self == ObjectType.FILE_FORMAT:
            return "FILE FORMAT"
        else:
            return self.value

    def singularize(self) -> str:
        return str(self)

    def pluralize(self) -> str:
        return f"{str(self)}S"
