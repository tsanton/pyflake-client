# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


@dataclass(frozen=True)
class Queryable(ISnowflakeDescribable):
    """Queryable"""

    query: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return self.query

    def is_procedure(self) -> bool:
        """is_procedure"""
        return False

    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
        return None
