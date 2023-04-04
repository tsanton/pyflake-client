"""snowflake_grant_interface"""
from abc import ABC, abstractmethod

from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.role import Role


class ISnowflakePrincipal(ABC):
    """ISnowflakePrinciple"""

    @abstractmethod
    def get_identifier(self) -> str:
        """get_identifier"""

    def get_snowflake_type(self) -> str:
        if isinstance(self, Role):
            return "ROLE"
        elif isinstance(self, DatabaseRole):
            return "DATABASE ROLE"

        raise NotImplementedError(f"Snowflake type for {self.__class__} is not defined")
