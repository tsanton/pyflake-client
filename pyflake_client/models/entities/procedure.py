# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.column_type import ColumnType


@dataclass(frozen=True)
class Procedure(ISnowflakeEntity):
    catalog_name: str
    schema_name: str
    name: str
    description: str
    procedure_args: List[ColumnType]
    created_on: datetime
