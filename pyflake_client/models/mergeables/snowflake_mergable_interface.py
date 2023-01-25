"""snowflake_mergable_interface"""
from abc import ABC, abstractmethod


class ISnowflakeMergable(ABC):
    """ISnowflakeMergable"""

    @abstractmethod
    def merge_into_statement(self, db_name: str, schema_name: str) -> str:
        """merge_into_statement"""

    @abstractmethod
    def select_statement(self, db_name: str, schema_name: str) -> str:
        """select_statement"""
