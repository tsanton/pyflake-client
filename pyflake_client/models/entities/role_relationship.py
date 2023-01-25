"""role_relationship"""
# pylint: disable=line-too-long
from dataclasses import dataclass


from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class RoleRelationship(ISnowflakeEntity):
    """RoleRelationship"""
    child_role_name: str
    parent_role_name: str
    grant_option: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this
