"""role_relationship"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Union

import dacite


from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class RoleRelationship(ISnowflakeEntity):
    """RoleRelationship"""

    child_role_name: str
    parent_role_name: str
    grant_option: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> RoleRelationship:
        return dacite.from_dict(data_class=cls, data=data, config=config)
