# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any

from pyflake_client.models.enums.column_type import ColumnType


@dataclass(frozen=True)
class ProcedureArg:
    positional_order: int
    data_type: ColumnType
    value: Any
