"""database"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Database(ISnowflakeEntity):
    """Database"""

    name: str
    owner: str
    origin: str
    comment: str
    retention_time: str
    created_on: datetime

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: dacite.Config) -> Database:
        return dacite.from_dict(data_class=cls, data=data, config=config)
