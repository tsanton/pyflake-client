# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import json
import time
from typing import Any, Callable, List, Type, TypeVar, Union

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import AsyncJob
from snowflake.snowpark.row import Row

T = TypeVar("T")


class AsyncCallJob:
    def __init__(self, original: AsyncJob):
        self._original = original

    def wait(self, timeout: int = 60) -> None:
        start_time = time.time()
        while not self.is_done():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job did not complete after {timeout} seconds.")
            time.sleep(0.1)

    def fetch_one(self, _: Type[T], deserializer: Union[Callable[[Any], T], None] = None) -> Union[T, None]:
        if deserializer is None and self._deserializer is None:
            raise ValueError("cannot fetch_one without any deserializer")
        try:
            rows: List[Row] = self._original.result()
        except ProgrammingError as e:
            print(e)
            return None
        if rows is None or len(rows) != 1:
            return None
        return deserializer(rows[0][0])

    def fetch_many(self, _: Type[T], deserializer: Union[Callable[[Any], T], None] = None) -> List[T]:
        if deserializer is None:
            raise ValueError("cannot fetch_all without any deserializer")
        try:
            rows: List[Row] = self._original.result()
        except ProgrammingError as e:
            print(e)
            return None
        if rows is None:
            return []
        data = [json.loads(r) for r in rows[0]][0]
        return [deserializer(r) for r in data]

    def is_done(self) -> bool:
        return self._original.is_done()

    def cancel(self) -> None:
        return self._original.cancel()
