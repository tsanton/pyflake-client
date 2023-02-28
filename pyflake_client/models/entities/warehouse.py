"""warehouse"""
from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Warehouse(ISnowflakeEntity):
    """Warehouse"""
    name: str
    owner: str
    type: str
    size: str
    auto_suspend: int
    auto_resume: str
    comment: str
    created_on: datetime
