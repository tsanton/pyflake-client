"""database"""
from dataclasses import dataclass
from datetime import datetime


from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Database(ISnowflakeEntity):
    """Database"""

    name: str
    owner: str
    origin: str
    comment: str
    retention_time: str
    created_on: datetime
