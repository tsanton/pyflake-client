"""schema_object_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class _SchemaObjectGrant:
    """SchemaObjectGrant"""
    role_name: str
    privileges: List[str]  # TODO: Enum privileges


@dataclass(frozen=True)
class SchemaObjectGrants(ISnowflakeEntity):
    """SchemaObjectGrants"""
    db_name: str
    schema_name: str
    object_type: ObjectType
    object_name: str
    grants: List[_SchemaObjectGrant] = None
