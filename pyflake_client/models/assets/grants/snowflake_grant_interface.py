"""snowflake_grant_interface"""
from abc import ABC, abstractmethod
from typing import List


class ISnowflakeGrant(ABC):
    """ISnowflakeRoleGrant"""

    @abstractmethod
    def get_grant_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_grant_statement"""

    @abstractmethod
    def get_revoke_statement(self, privileges: List[str]) -> str:  # TODO: List[enum]
        """get_revoke_statement"""

    @abstractmethod
    def validate(self, privileges: List[str]) -> bool:  # TODO: List[enum]
        """validates that all privileges are valid for the grant target"""
