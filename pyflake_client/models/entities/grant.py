"""grant"""
from dataclasses import dataclass

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Grant(ISnowflakeEntity):
    """Grant"""

    privilege: str
    granted_on: str
    name: str
    grant_option: str
    granted_by: str
