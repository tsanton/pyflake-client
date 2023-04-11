from dataclasses import dataclass

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class FutureGrant(ISnowflakeEntity):
    privilege: str
    grant_on: str
    name: str
    grant_option: str