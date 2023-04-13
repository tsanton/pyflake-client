"""schema"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Union

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Schema(ISnowflakeEntity):
    """Schema"""
    name: str
    database_name: str
    owner: str
    comment: str
    retention_time: int
    created_on: datetime


    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> Schema:
        data["retention_time"] = int(data["retention_time"])
        return dacite.from_dict(data_class=cls, data=data, config=config)