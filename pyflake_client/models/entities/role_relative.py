"""role_relative"""
from dataclasses import dataclass


@dataclass(frozen=True)
class RoleRelative:
    """RoleRelative"""

    role_name: str
    parent_role_name: str
    distance_from_source: int
    grant_option: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this
