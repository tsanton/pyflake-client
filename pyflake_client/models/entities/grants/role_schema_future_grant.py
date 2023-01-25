"""role_schema_future_grants"""
from dataclasses import dataclass
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class _SchemaObjectFutureGrant:
    """SchemaObjectFutureGrant"""
    grant_target: ObjectType
    privileges: List[str]


@dataclass(frozen=True)
class RoleSchemaFutureGrants(ISnowflakeEntity):
    """RoleSchemaObjectFutureGrants"""
    role_name: str
    db_name: str
    schema_name: str
    future_grants: List[_SchemaObjectFutureGrant]
