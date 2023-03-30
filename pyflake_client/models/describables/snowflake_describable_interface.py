"""snowflake_describable_interface"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Dict
from dacite import Config
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
    def get_dacite_config(self) -> Config:
        """get_dacite_config"""

    @staticmethod
    def load_from_sf(
        cls_, data: Dict[str, Any], config: dacite.Config
    ) -> ISnowflakeDescribable:
        return dacite.from_dict(data_class=cls_, data=data, config=config)
