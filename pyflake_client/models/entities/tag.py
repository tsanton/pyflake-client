from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass
class Tag(ISnowflakeEntity):
    database_name: str
    schema_name: str
    name: str
    owner: str
    comment: str
    allowed_values: Union[List[str], None]
    created_on: datetime

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None
    ) -> Tag:
        return Tag(
            database_name=data["database_name"],
            schema_name=data["schema_name"],
            name=data["name"],
            owner=data["owner"],
            comment=data["comment"],
            allowed_values=data.get("allowed_values", []),
            created_on=data["created_on"],
        )
