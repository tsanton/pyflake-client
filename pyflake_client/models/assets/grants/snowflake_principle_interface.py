"""snowflake_grant_interface"""
from abc import ABC, abstractmethod
from typing import List

class ISnowflakePrinciple(ABC):
    """ISnowflakePrinciple"""
    @abstractmethod
    def get_identifier(self) -> str:
        """get_identifier"""