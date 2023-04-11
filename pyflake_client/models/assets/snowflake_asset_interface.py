"""snowflake_asset_interface"""
from abc import ABC, abstractmethod


class ISnowflakeAsset(ABC):
    """ISnowflakeAsset"""

    @abstractmethod
    def get_create_statement(self) -> str:
        """get_create_statement"""

    @abstractmethod
    def get_delete_statement(self) -> str:
        """get_delete_statement"""
