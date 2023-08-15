# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class ISnowflakeExecutable(ABC):
    @abstractmethod
    def get_call_statement(self) -> str:
        ...
