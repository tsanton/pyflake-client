"""role"""
from dataclasses import dataclass
from datetime import datetime
from typing import Union

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Role(ISnowflakeEntity):
    """Role"""
    name: str
    owner: Union[str, None] = None
    assigned_to_users: Union[int, None] = None
    granted_to_roles: Union[int, None] = None
    granted_roles: Union[int, None] = None
    comment: Union[str, None] = None
    created_on: Union[datetime, None] = None
