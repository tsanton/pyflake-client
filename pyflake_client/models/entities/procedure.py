"""procedure"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Union

import dacite


from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.utils.parse_sf_types import parse_sf_datetime


@dataclass(frozen=True)
class Procedure(ISnowflakeEntity):
    """Procedure"""
    catalog_name: str
    schema_name: str
    name: str
    description: str
    procedure_args: List[str]
    created_on: datetime

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None) -> Procedure:
        data["created_on"] = parse_sf_datetime(data["created_on"])
        return Procedure(**{k: data[k] for k in cls.__dataclass_fields__})