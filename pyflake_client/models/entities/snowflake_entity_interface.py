# -*- coding: utf-8 -*-
from abc import ABC
from typing import Any, Dict, Type, TypeVar, Union

import dacite

T = TypeVar("T", bound="ISnowflakeEntity")


class ISnowflakeEntity(ABC):
    """ISnowflakeEntity"""

    @classmethod
    def deserialize(cls, data: Dict[str, Any], config: Union[dacite.Config, None]) -> Type[T]:
        return dacite.from_dict(data_class=cls, data=data, config=config)
