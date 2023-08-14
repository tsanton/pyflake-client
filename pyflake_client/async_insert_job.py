# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import time
from typing import TypeVar

from snowflake.connector.errors import ProgrammingError
from snowflake.snowpark import AsyncJob
from snowflake.snowpark.row import Row

T = TypeVar("T")

class AsyncInsertJob:
    def __init__(self, original: AsyncJob):
        self._original = original

    def wait(self, timeout: int = 60) -> None:
        start_time = time.time()
        while not self.is_done():
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Job did not complete after {timeout} seconds.")
            time.sleep(0.1)

    def is_done(self) -> bool:
        return self._original.is_done()

    def cancel(self) -> None:
        return self._original.cancel()
