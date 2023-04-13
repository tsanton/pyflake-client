"""principal_ascendants"""
from __future__ import annotations
from typing import Any, Union, Dict
from typing import List
from dataclasses import dataclass
from datetime import datetime

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.role_type import RoleType
from pyflake_client.utils.parse_sf_types import parse_sf_datetime


@dataclass(frozen=True)
class PrincipalAscendant:
    """PrincipalAscendant"""
    grantee_identifier: str
    principal_type: RoleType
    granted_identifier: str
    granted_on: str
    granted_by: str
    created_on: datetime
    distance_from_source: int

    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "grantee_name": "grantee_identifier",
            "granted_to": "principal_type",
            "role": "granted_identifier",
        }

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> PrincipalAscendant:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)

        data["created_on"] = parse_sf_datetime(data["created_on"])
        data["principal_type"] = RoleType[data["principal_type"]]
        return PrincipalAscendant(**{k: data[k] for k in cls.__dataclass_fields__})

@dataclass(frozen=True)
class PrincipalAscendants(ISnowflakeEntity):
    """PrincipalAscendants"""
    principal_identifier: str
    principal_type: RoleType
    ascendants: List[PrincipalAscendant]

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None]) -> PrincipalAscendants:
        ascendants = [PrincipalAscendant.load_from_sf(c) for c in data["ascendants"]]
        data["principal_type"] = RoleType[data["principal_type"]]
        return PrincipalAscendants(
            principal_identifier=data["principal_identifier"],
            principal_type=data["principal_type"],
            ascendants=ascendants
        )