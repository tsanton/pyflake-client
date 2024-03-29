# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod

from pyflake_client.models.enums.principal import Principal


class ISnowflakePrincipal(ABC):
    @abstractmethod
    def get_identifier(self) -> str:
        ...

    def get_snowflake_type(self) -> Principal:
        raise NotImplementedError(f"Snowflake type for {self.__class__} is not defined")
