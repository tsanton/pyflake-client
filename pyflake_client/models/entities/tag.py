from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass
class Tag(ISnowflakeEntity):
    database_name: str
    schema_name: str
    name: str
    owner: str
    comment: str
    allowed_values: List[str]
    created_on: datetime
