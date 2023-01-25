"""role_future_grant"""
from dataclasses import dataclass
from typing import List
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class _RoleFutureGrant:
    """RoleFutureGrant"""
    privilege: str
    grant_on: str
    name: str
    grant_option: str


@dataclass(frozen=True)
class RoleFutureGrants(ISnowflakeEntity):
    """RoleFutureGrants"""
    role_name: str
    grants: List[_RoleFutureGrant]
