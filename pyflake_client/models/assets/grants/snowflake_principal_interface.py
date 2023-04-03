"""snowflake_grant_interface"""
from abc import ABC, abstractmethod


class ISnowflakePrincipal(ABC):
    """ISnowflakePrinciple"""

    @abstractmethod
    def get_identifier(self) -> str:
        """get_identifier"""
