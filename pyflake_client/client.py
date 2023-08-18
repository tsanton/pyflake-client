# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
from typing import List, Union

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session

from pyflake_client.async_asset_job import AsyncAssetJob, AsyncAwaitable
from pyflake_client.async_call_job import AsyncCallJob
from pyflake_client.async_describe_job import AsyncDescribeJob
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)


class PyflakeClient:
    def __init__(self, conn: SnowflakeConnection) -> None:
        self._conn: SnowflakeConnection = conn
        self._session = Session.builder.config("connection", conn).create()

    def wait_all(self, waiters: List[AsyncAwaitable], timeout: int = 60) -> None:
        for waiter in waiters:
            waiter.wait()

    def execute_async(self, statement: str) -> AsyncCallJob:
        return AsyncCallJob(original=self._session.sql(statement).collect_nowait())

    def create_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        return self._create_asset_async(obj, None)

    def register_asset_async(self, obj: ISnowflakeAsset, asset_queue: queue.LifoQueue) -> AsyncAssetJob:
        return self._create_asset_async(obj, asset_queue)

    def _create_asset_async(self, obj: ISnowflakeAsset, asset_queue: Union[queue.LifoQueue, None]) -> AsyncAssetJob:
        cur = self._conn.cursor()
        cur.execute(obj.get_create_statement().strip(), num_statements=0, _exec_async=True)
        return AsyncAssetJob(conn=self._conn, cursor=cur, query_id=cur.sfqid, asset=obj, queue=asset_queue)

    def delete_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        return self._delete_asset_async(obj)

    def delete_assets(self, asset_queue: queue.LifoQueue[ISnowflakeAsset]) -> None:
        statements = []
        while not asset_queue.empty():
            statements.append(asset_queue.get().get_delete_statement().strip())
        with self._conn.cursor() as cur:
            cur.execute(";\n".join(statements), num_statements=0)

    def _delete_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        cur = self._conn.cursor()
        statement = obj.get_delete_statement().strip()
        cur.execute(statement, num_statements=0, _exec_async=True)
        return AsyncAssetJob(conn=self._conn, cursor=cur, query_id=cur.sfqid, asset=obj, queue=None)

    def describe_async(self, describable: ISnowflakeDescribable) -> AsyncDescribeJob:
        """The ISnowflakeDescribable must contain a single statement query (1x';')"""
        return AsyncDescribeJob(
            original=self._session.sql(describable.get_describe_statement()).collect_nowait(),
            is_procedure=describable.is_procedure(),
            deserializer=describable.get_deserializer(),
        )
