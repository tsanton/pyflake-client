"""snowflake_entity_interface"""
from abc import ABC
from typing import Dict, Any, TypeVar, Type, Union

import dacite

T = TypeVar("T", bound="ISnowflakeEntity")


class ISnowflakeEntity(ABC):
    """ISnowflakeEntity"""

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> Type[T]:
        return dacite.from_dict(data_class=cls, data=data, config=config)  # type: ignore
