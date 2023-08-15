# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod


class ISnowflakeAsset(ABC):
    @abstractmethod
    def get_create_statement(self) -> str:
        ...

    @abstractmethod
    def get_delete_statement(self) -> str:
        ...
