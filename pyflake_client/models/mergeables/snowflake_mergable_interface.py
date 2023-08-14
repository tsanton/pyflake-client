"""snowflake_mergable_interface"""
from abc import ABC, abstractmethod


class ISnowflakeMergable(ABC):
    """ISnowflakeMergable"""

    @abstractmethod
    def merge_into_statement(self) -> str:
        """merge_into_statement"""

    @abstractmethod
    def select_statement(self) -> str:
        """select_statement"""
