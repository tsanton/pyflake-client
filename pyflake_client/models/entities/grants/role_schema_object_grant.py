"""role_schema_object_grant"""
from dataclasses import dataclass
from typing import List

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class _ObjectPrivilege:
    """ObjectPrivilegeDto"""
    role_name: str
    privileges: List[str]  # TODO: Enum privileges


@dataclass(frozen=True)
class RoleSchemaObjectGrants(ISnowflakeEntity):
    """RoleSchemaObjectGrants"""
    role_name: str
    db_name: str
    schema_name: str
    object_type: ObjectType
    object_name: str
    granted_privileges: List[str] = None
    inherited_privileges: List[_ObjectPrivilege] = None
    all_privileges: List[str] = None
