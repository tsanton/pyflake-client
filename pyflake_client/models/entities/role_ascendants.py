"""role_ascendants"""
# pylint: disable=line-too-long
from typing import List
from dataclasses import dataclass
from pyflake_client.models.entities.role_relative import RoleRelative

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class RoleAscendants(ISnowflakeEntity):
    """RoleAscendants"""
    name: str
    ascendant_roles: List[RoleRelative]
