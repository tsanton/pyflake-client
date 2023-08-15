# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass
class Tag(ISnowflakeEntity):
    database_name: str
    schema_name: str
    name: str
    owner: str
    comment: str
    allowed_values: Union[List[str], None]
    created_on: datetime
