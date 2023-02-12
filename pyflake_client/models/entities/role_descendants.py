"""role_descendants"""
from typing import List
from dataclasses import dataclass
from pyflake_client.models.entities.role_relative import RoleRelative

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class RoleDescendants(ISnowflakeEntity):
    """RoleDescendants"""
    name: str
    descendant_roles: List[RoleRelative]
