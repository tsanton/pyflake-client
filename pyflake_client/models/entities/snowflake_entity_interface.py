"""snowflake_entity_interface"""
from abc import ABC
from typing import Dict, Any, TypeVar, Type

import dacite

T = TypeVar("T", bound="ISnowflakeEntity")


class ISnowflakeEntity(ABC):
    """ISnowflakeEntity"""

    @classmethod
    def load_from_sf(cls, data: Dict[str, Any], config: dacite.Config) -> Type[T]:
        ...
