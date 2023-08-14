# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

import dacite


class ISnowflakeDescribable(ABC):
    """ISnowflakeDescribable"""

    @abstractmethod
    def get_describe_statement(self) -> str:
        """get_describe_statement"""

    @abstractmethod
    def is_procedure(self) -> bool:
        """is_procedure"""

    @abstractmethod
    def get_dacite_config(self) -> Union[dacite.Config, None]:
        """get_dacite_config"""
