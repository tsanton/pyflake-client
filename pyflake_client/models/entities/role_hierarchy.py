"""role_hierarchy"""
# pylint: disable=line-too-long
from typing import List
from dataclasses import dataclass

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class InheritedRole:
    """InheritedRole"""
    role_name: str
    parent_role_name: str
    distance_from_source: int
    grant_option: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this


@dataclass(frozen=True)
class RoleHierarchy(ISnowflakeEntity):
    """RoleHierarchy"""
    name: str
    inheriting_roles: List[InheritedRole]
