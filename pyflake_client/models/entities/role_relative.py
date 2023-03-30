"""role_relative"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Union

import dacite


@dataclass(frozen=True)
class RoleRelative:
    """RoleRelative"""

    role_name: str
    parent_role_name: str
    distance_from_source: int
    grant_option: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> RoleRelative:
        return dacite.from_dict(data_class=cls, data=data, config=config)
