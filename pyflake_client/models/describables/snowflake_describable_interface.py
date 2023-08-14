# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, TypeVar

T = TypeVar("T")


class ISnowflakeDescribable(ABC):
    """ISnowflakeDescribable"""

    @abstractmethod
    def get_describe_statement(self) -> str:
        """get_describe_statement"""

    @abstractmethod
    def is_procedure(self) -> bool:
        """is_procedure"""

    @classmethod
    def get_deserializer(self) -> Callable[[Dict[str, Any]], T]:
        """get_deserializer"""
