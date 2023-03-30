"""procedure"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Union

import dacite

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class Procedure(ISnowflakeEntity):
    """Procedure"""

    catalog_name: str
    schema_name: str
    name: str
    description: str
    procedure_args: List[str]
    created_on: str

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> Procedure:
        return dacite.from_dict(data_class=cls, data=data, config=config)
