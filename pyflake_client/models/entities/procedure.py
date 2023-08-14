# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Procedure(ISnowflakeEntity):
    """Procedure"""

    catalog_name: str
    schema_name: str
    name: str
    description: str
    procedure_args: List[str]
    created_on: str  # TODO: Datetime
