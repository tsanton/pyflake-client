# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Callable, Dict, Union

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Queryable(ISnowflakeDescribable):
    query: str
    is_proc: bool = False

    def get_describe_statement(self) -> str:
        return self.query

    def is_procedure(self) -> bool:
        return self.is_proc

    @classmethod
    def get_deserializer(cls) -> Union[Callable[[Dict[str, Any]], Any], None]:
        return None
