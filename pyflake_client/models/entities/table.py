# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyflake_client.models.entities.classification_tag import ClassificationTag
from pyflake_client.models.entities.column import Column as ColumnEntity
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Table(ISnowflakeEntity):
    name: str
    database_name: str
    schema_name: str
    kind: str
    columns: List[ColumnEntity]
    comment: str
    tags: List[ClassificationTag]
    rows: int
    owner: str
    retention_time: int
    created_on: datetime
