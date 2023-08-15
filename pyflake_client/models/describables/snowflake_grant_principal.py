# -*- coding: utf-8 -*-
from abc import ABC


class ISnowflakeGrantPrincipal(ABC):
    @staticmethod
    def get_snowflake_type() -> str:
        ...
