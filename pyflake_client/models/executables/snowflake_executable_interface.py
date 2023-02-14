"""snowflake_executable_interface"""
from abc import ABC, abstractmethod


class ISnowflakeExecutable(ABC):
    """ISnowflakeExecutable"""

    @abstractmethod
    def get_call_statement(self) -> str:
        """get_call_statement"""
