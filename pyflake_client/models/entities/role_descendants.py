"""role_descendants"""
from __future__ import annotations
from typing import Any, Dict, List, Union
from dataclasses import dataclass

import dacite
from pyflake_client.models.entities.role_relative import RoleRelative

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class RoleDescendants(ISnowflakeEntity):
    """RoleDescendants"""

    name: str
    descendant_roles: List[RoleRelative]

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> RoleDescendants:
        return dacite.from_dict(data_class=cls, data=data, config=config)
