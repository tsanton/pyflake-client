"""table"""
# pylint: disable=line-too-long

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Table(ISnowflakeEntity):
    """Table"""
    name: str
    database_name: str
    schema_name: str
    kind: str
    comment: str
    rows: int
    owner: str
    retention_time: str
    created_on: datetime
