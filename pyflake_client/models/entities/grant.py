"""grant"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Union

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.privilege import Privilege
from pyflake_client.utils.parse_sf_types import parse_sf_bool, parse_sf_datetime


@dataclass(frozen=True)
class Grant(ISnowflakeEntity):
    """Grant"""
    grantee_identifier: str
    grantee_type: str  #TODO: Enum?
    granted_on: str
    granted_identifier: str
    privilege: Privilege
    grant_option: bool
    granted_by: str
    created_on: datetime


    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "grantee_name": "grantee_identifier",
            "granted_to": "grantee_type",
            "name": "granted_identifier",
        }

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> Grant:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)
        
        data["grant_option"] = parse_sf_bool(data["grant_option"])
        return Grant(**{k: data[k] for k in cls.__dataclass_fields__})