"""table"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import dacite

from pyflake_client.models.entities.column import Column as ColumnEntity
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class ClassificationTag:
    tag_database_name: str
    tag_schema_name: str
    tag_name: str
    domain_level: str
    tag_value: Union[str, None] = None

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None
    ) -> ClassificationTag:
        return ClassificationTag(
            tag_database_name=data["TAG_DATABASE"],
            tag_schema_name=data["TAG_SCHEMA"],
            tag_name=data["TAG_NAME"],
            domain_level=data["DOMAIN"],
            tag_value=data["TAG_VALUE"],
        )


@dataclass(frozen=True)
class Table(ISnowflakeEntity):
    """Table"""

    name: str
    database_name: str
    schema_name: str
    kind: str
    columns: List[ColumnEntity]
    comment: str
    tags: List[ClassificationTag]
    rows: int
    owner: str
    retention_time: str
    created_on: datetime

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> Table:
        columns = [ColumnEntity.load_from_sf(c) for c in data["columns"]]
        tags = [ClassificationTag.load_from_sf(t) for t in data["tags"]]
        return Table(
            name=data["name"],
            database_name=data["database_name"],
            schema_name=data["schema_name"],
            kind=data["kind"],
            columns=columns,
            comment=data["comment"],
            tags=tags,
            rows=data["rows"],
            owner=data["owner"],
            retention_time=data["retention_time"],
            created_on=data["created_on"],
        )
