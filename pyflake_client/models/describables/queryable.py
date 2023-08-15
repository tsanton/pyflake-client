# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict, Union

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Queryable(ISnowflakeDescribable):
    """Queryable"""

    query: str
    is_procedure: bool = False

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return self.query

    def is_procedure(self) -> bool:
        """is_procedure"""
        return self.is_procedure

    @classmethod
    def get_deserializer(cls) -> Union[Callable[[Dict[str, Any]], Any], None]:
        return None
