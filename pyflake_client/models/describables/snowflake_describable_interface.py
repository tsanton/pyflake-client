"""ISnowflakeDescribable"""
from abc import ABC, abstractmethod
from dacite import Config


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
