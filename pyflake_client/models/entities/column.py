from __future__ import annotations
from abc import ABC
from dataclasses import dataclass
import re
from typing import Any, Dict, List, Union

import dacite


from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.entities.classification_tag import ClassificationTag


@dataclass
class Column(ISnowflakeEntity, ABC):
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

    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "null?": "nullable",
            "policy name": "policy_name",
            "primary key": "primary_key",
            "unique key": "unique_key",
        }

    @classmethod
    def deserialize(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None
    ) -> Column:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)

        data["tags"] = [ClassificationTag.deserialize(tag, config) for tag in data["tags"]]
        data["primary_key"] = data["primary_key"] == "Y"
        data["unique_key"] = data["unique_key"] == "Y"
        data["nullable"] = data["nullable"] == "Y"

        defaults = {k: data[k] for k in cls.__dataclass_fields__}
        data_type = data["type"]
        if re.match(r"NUMBER\([0-9]{1,2},[0-9]{1,2}\)", data_type):
            return Number(
                precision=data["data_type"]["precision"],
                scale=data["data_type"]["scale"],
                **defaults,
            )
        elif re.match(r"VARCHAR\([0-9]{1,8}\)", data_type):
            return Varchar(length=data["data_type"]["length"], **defaults)
        elif re.match(r"TIME\([0-9]\)", data_type):
            return Time(precision=data["data_type"]["precision"], **defaults)
        elif re.match(r"TIMESTAMP_NTZ\([0-9]\)", data_type):
            return Timestamp(precision=data["data_type"]["precision"], **defaults)
        elif data_type == "BOOLEAN":
            return Bool(**defaults)
        elif data_type == "DATE":
            return Date(**defaults)
        elif data_type == "VARIANT":
            return Variant(**defaults)
        raise ValueError(f"ColumnEntitiy received undefined data type: {data_type}")


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
