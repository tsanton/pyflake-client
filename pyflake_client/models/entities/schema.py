"""schema"""
from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Schema(ISnowflakeEntity):
    """Schema"""
    name: str
    database_name: str
    owner: str
    comment: str
    retention_time: str  #TODO: int
    created_on: datetime
