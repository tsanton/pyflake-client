"""table"""
# pylint: disable=line-too-long

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import dacite

from pyflake_client.models.entities.column import Column as ColumnEntity
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Table(ISnowflakeEntity):
    """Table"""

    name: str
    database_name: str
    schema_name: str
    kind: str
    columns: List[ColumnEntity]
    comment: str
    rows: int
    owner: str
    retention_time: str
    created_on: datetime

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> Table:
        return dacite.from_dict(cls, data, config=config)
