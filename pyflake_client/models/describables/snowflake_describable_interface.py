"""snowflake_describable_interface"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict, Union
import dacite


class ISnowflakeDescribable(ABC):
    """ISnowflakeDescribable"""

    @abstractmethod
    def get_describe_statement(self) -> str:
        """get_describe_statement"""

    @abstractmethod
    def is_procedure(self) -> bool:
        """is_procedure"""

    @abstractmethod
    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""

    @staticmethod
    def load_from_sf(
        cls_, data: Dict[str, Any], config: Union[dacite.Config, None]
    ) -> ISnowflakeDescribable:
        return dacite.from_dict(data_class=cls_, data=data, config=config)
