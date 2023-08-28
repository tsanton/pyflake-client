# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, TypeVar

T = TypeVar("T", bound='ISnowflakeMergable')


class ISnowflakeMergable(ABC, Generic[T]):
    def configure(self, db_name: str, schema_name: str, table_name: str) -> T:
        self._db_name = db_name
        self._schema_name = schema_name
        self._table_name = table_name

        return self

    @abstractmethod
    def merge_into_statement(self) -> str:
        ...

    @abstractmethod
    def select_statement(self) -> str:
        ...

    @classmethod
    def get_deserializer(self) -> Callable[[Dict[str, Any]], T]:
        ...

    
