# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import json
import time
from typing import Any, Dict, List, Type, TypeVar, Union

import dacite
from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import AsyncJob
from snowflake.snowpark.row import Row

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity

T = TypeVar("T", bound=ISnowflakeEntity)


class AsyncDescribeJob:
    def __init__(self, original: AsyncJob, is_procedure: bool, config: Union[dacite.Config, None]):
        self._original = original
        self._is_procedure = is_procedure
        self._config = config

    def wait(self, timeout: int = 60) -> None:
        start_time = time.time()
        while not self.is_done():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job did not complete after {timeout} seconds.")
            time.sleep(0.1)
        res = self._original.result()
        print(res)

    def deserialize_one(self, entity: Type[T], config: Union[dacite.Config, None] = None) -> Union[T, None]:
        data: Dict[str, Any] = {}
        try:
            rows: List[Row] = self._original.result()
        except ProgrammingError as e:
            print(e)
            return None
        if rows is None or len(rows) != 1:
            return None
        if self._is_procedure:
            data = json.loads(rows[0][0])
            if data in ({}, []) or data is None:
                return None
        else:
            data = rows[0].as_dict()
        return entity.deserialize(data=data, config=config if config is not None else self._config)

    def deserialize_many(self, entity: Type[T], config: Union[dacite.Config, None] = None) -> Union[List[T], None]:
        try:
            rows: List[Row] = self._original.result()
        except ProgrammingError as e:
            print(e)
            return []
        if rows is None:
            return []
        if self._is_procedure:
            data = [json.loads(r) for r in rows[0]][0]
            if data in ({}, []):
                return []
            return [entity.deserialize(data=x, config=config if config is not None else self._config) for x in data]
        return [
            entity.deserialize(data=x.as_dict(), config=config if config is not None else self._config) for x in rows
        ]

    def is_done(self) -> bool:
        return self._original.is_done()

    def cancel(self) -> None:
        return self._original.cancel()
