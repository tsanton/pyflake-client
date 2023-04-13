"""role_inheritance"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Union

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.utils.parse_sf_types import parse_sf_bool, parse_sf_datetime


@dataclass(frozen=True)
class RoleInheritance(ISnowflakeEntity):
    """RoleInheritance"""
    principal_identifier: str
    principal_type: str
    inherited_role_identifier: str
    inherited_role_type: str #TODO: enum?
    privilege: str #TODO: enum
    grant_option: bool
    granted_by: str
    created_on: datetime


    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "grantee_name": "principal_identifier",
            "granted_to": "principal_type",
            "name": "inherited_role_identifier",
            "granted_on": "inherited_role_type",
        }

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> RoleInheritance:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)

        data["created_on"] = parse_sf_datetime(data["created_on"])
        data["grant_option"] = parse_sf_bool(data["grant_option"])
        return RoleInheritance(**{k: data[k] for k in cls.__dataclass_fields__})