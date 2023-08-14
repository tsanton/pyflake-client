# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class ISnowflakeExecutable(ABC):
    """ISnowflakeExecutable"""

    @abstractmethod
    def get_call_statement(self) -> str:
        """get_call_statement"""
