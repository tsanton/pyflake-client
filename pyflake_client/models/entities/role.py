"""role"""
from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Role(ISnowflakeEntity):
    """Role"""
    name: str
    owner: str = None
    assigned_to_users: int = None
    granted_to_roles: int = None
    granted_roles: int = None
    comment: str = None
    created_on: datetime = None
