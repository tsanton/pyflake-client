"""principal_descendants"""
from __future__ import annotations
from typing import Any, Dict, List, Union
from dataclasses import dataclass

import dacite
from pyflake_client.models.entities.grant import Grant

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.role_type import RoleType

@dataclass(frozen=True)
class PrincipalDescendants(ISnowflakeEntity):
    """PrincipalDescendants"""
    principal_identifier: str
    principal_type: RoleType
    descendants: List[Grant]

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None]) -> PrincipalDescendants:
        descendants = [Grant.load_from_sf(c) for c in data["descendants"]]
        data["principal_type"] = RoleType[data["principal_type"]]
        return PrincipalDescendants(
            principal_identifier=data["principal_identifier"],
            principal_type=data["principal_type"],
            descendants=descendants,
        )