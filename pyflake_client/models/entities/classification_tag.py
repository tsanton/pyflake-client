# -*- coding: utf-8 -*-

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class ClassificationTag(ISnowflakeEntity):
    tag_database_name: str
    tag_schema_name: str
    tag_name: str
    domain_level: str
    tag_value: Union[str, None] = None
