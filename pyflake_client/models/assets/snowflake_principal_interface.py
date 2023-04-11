"""snowflake_principal_interface"""
from abc import ABC, abstractmethod


class ISnowflakePrincipal(ABC):
    """ISnowflakePrincipal"""

    @abstractmethod
    def get_identifier(self) -> str:
        """get_identifier"""

    def get_snowflake_type(self) -> str:
        raise NotImplementedError(f"Snowflake type for {self.__class__} is not defined")
