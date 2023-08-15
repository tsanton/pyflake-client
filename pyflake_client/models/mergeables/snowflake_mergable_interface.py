# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, TypeVar

T = TypeVar("T")


class ISnowflakeMergable(ABC):
    @abstractmethod
    def merge_into_statement(self) -> str:
        ...

    @abstractmethod
    def select_statement(self) -> str:
        ...

    @classmethod
    def get_deserializer(self) -> Callable[[Dict[str, Any]], T]:
        ...
