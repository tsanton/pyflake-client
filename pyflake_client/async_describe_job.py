# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import json
import time
from typing import Any, Callable, Dict, List, Type, TypeVar, Union

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import AsyncJob
from snowflake.snowpark.row import Row

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity

T = TypeVar("T", bound=ISnowflakeEntity)


class AsyncDescribeJob:
    def __init__(self, original: AsyncJob, is_procedure: bool, deserializer: Callable[[Dict[str, Any]], T]):
        self._original = original
        self._is_procedure = is_procedure
        self._deserializer = deserializer

    def wait(self, timeout: int = 60) -> None:
        start_time = time.time()
        while not self.is_done():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job did not complete after {timeout} seconds.")
            time.sleep(0.1)

    def deserialize_one(self, _: Type[T], deserializer: Union[Callable[[Dict[str, Any]], T], None] = None) -> Union[T, None]:
        if self._deserializer is None and deserializer is None:
            raise ValueError("cannot deserialize_one without any deserializer")
        deserializer_func = deserializer if deserializer is not None else self._deserializer
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
        return deserializer_func(data)

    def deserialize_many(self, _: Type[T], deserializer: Union[Callable[[Dict[str, Any]], T], None] = None) -> List[T]:
        if self._deserializer is None and deserializer is None:
            raise ValueError("cannot deserialize_many without any deserializer")
        deserializer_func = deserializer if deserializer is not None else self._deserializer
        try:
            rows: List[Row] = self._original.result()
        except ProgrammingError as e:
            print(e)
            return []
        if rows is None:
            return []
        if self._is_procedure:
            data = [json.loads(r) for r in rows[0]][0]
            if data in ({}, []) or data is None:
                return []
            return [deserializer_func(x) for x in data]
        return [
            deserializer_func(x.as_dict()) for x in rows
        ]

    def is_done(self) -> bool:
        return self._original.is_done()

    def cancel(self) -> None:
        return self._original.cancel()
