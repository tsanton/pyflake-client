"""future_grant"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Union

import dacite
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass(frozen=True)
class FutureGrant(ISnowflakeEntity):
    grantee_identifier: str
    grantee_type: str #TODO: Enum?
    grant_on: str
    grant_identifier: str
    privilege: Privilege
    grant_option: str #TODO: Bool
    created_on: str  # TODO: datetime, not string.

    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "grantee_name": "grantee_identifier",
            "grant_to": "grantee_type",
            "name": "grant_identifier",
        }

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> FutureGrant:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)
        return FutureGrant(**{k: data[k] for k in cls.__dataclass_fields__})