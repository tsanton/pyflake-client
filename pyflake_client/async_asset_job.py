# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
import time
from abc import ABC, abstractmethod
from typing import Union

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset


class AsyncAwaitable(ABC):
    @abstractmethod
    def wait(self, timeout: int = 60) -> None:
        """wait"""


class AsyncAssetJob(AsyncAwaitable):
    def __init__(
        self,
        conn: SnowflakeConnection,
        cursor: SnowflakeCursor,
        query_id: str,
        asset: Union[ISnowflakeAsset, None],
        queue: Union[queue.LifoQueue, None],
    ):
        self._conn = conn
        self._cur = cursor
        self._query_id = query_id
        self._asset = asset
        self._queue = queue
        self._result = None

    def wait(self, timeout: int = 60) -> None:
        start_time = time.time()
        while True:
            status = self._conn.get_query_status(self._query_id)
            if status == QueryStatus.SUCCESS:
                return
            elif self._conn.is_an_error(status):
                raise ProgrammingError(status)
            elif status != QueryStatus.SUCCESS and not self._conn.is_still_running(status):
                # If status is neither running nor success, it's an unknown state.
                raise ProgrammingError(f"Status of query '{self._query_id}' is {status.name}, results are unavailable")
            # Check for timeout
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Not all queries completed after {timeout} seconds.")
            # Sleep for a short duration before checking again, to not hog the CPU
            time.sleep(0.1)

    def cancel(self) -> bool:
        self._cur.abort_query(self._query_id)
        self._cur.close()
