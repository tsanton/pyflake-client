"""role_grant"""
from dataclasses import dataclass
from typing import List
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class _RoleGrant:
    """RoleGrant"""
    privilege: str
    granted_on: str
    name: str
    grant_option: str
    granted_by: str


@dataclass(frozen=True)
class RoleGrants(ISnowflakeEntity):
    """RoleGrants"""
    role_name: str
    grants: List[_RoleGrant]
