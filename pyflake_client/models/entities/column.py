# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Union

from pyflake_client.models.entities.classification_tag import ClassificationTag
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass
class Column(ISnowflakeEntity):
    name: str
    type: str
    auto_increment: Union[str, None]
    check: Union[str, None]
    comment: Union[str, None]
    default: Union[str, None]
    expression: Union[str, None]
    tags: List[ClassificationTag]
    kind: str
    nullable: bool
    policy_name: Union[str, None]
    primary_key: bool
    unique_key: bool


@dataclass
class Number(Column):
    precision: int
    scale: int


@dataclass
class Bool(Column):
    ...


@dataclass
class Varchar(Column):
    length: int


@dataclass
class Date(Column):
    ...


@dataclass
class Time(Column):
    precision: int


@dataclass
class Timestamp(Column):
    precision: int


@dataclass
class Variant(Column):
    ...
