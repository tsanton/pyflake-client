"""pyflake_client"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
from typing import List, TypeVar, Union

from snowflake.connector import SnowflakeConnection
from snowflake.snowpark import Session

from pyflake_client.async_asset_job import AsyncAssetJob, AsyncAwaitable
from pyflake_client.async_describe_job import AsyncDescribeJob
from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable
from pyflake_client.models.mergeables.snowflake_mergable_interface import ISnowflakeMergable



U = TypeVar("U", bound=ISnowflakeMergable)
V = TypeVar("V")




class PyflakeClient:
    """PyflakeClient"""

    def __init__(self, conn: SnowflakeConnection) -> None:
        self._conn: SnowflakeConnection = conn
        self._session = Session.builder.config("connection", conn).create()

    
    def wait_all(self, waiters: List[AsyncAwaitable], timeout: int = 60) -> None:
        for waiter in waiters:
            waiter.wait()

    # def execute_scalar(self, query: str) -> Any:
    #     """execute_scalar"""
    #     with self._conn.cursor() as cur:
    #         cur.execute(query)
    #         row = cur.fetchone()
    #         if not row:
    #             return None
    #         return row[0]

    # def execute(self, executable: ISnowflakeExecutable) -> Any:
    #     """execute"""
    #     with self._conn.cursor() as cur:
    #         cur.execute(executable.get_call_statement())
    #         data = cur.fetchall()
    #         if not data:
    #             return None
    #         if len(data) == 1:
    #             return data[0][0]
    #         return [x[0] for x in data]

    def create_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        """create_asset"""
        return self._create_asset_async(obj, None)
        

    def register_asset_async(self, obj: ISnowflakeAsset, asset_queue: queue.LifoQueue) -> AsyncAssetJob:
        """register_asset"""
        return self._create_asset_async(obj, asset_queue)

    def _create_asset_async(self, obj: ISnowflakeAsset, asset_queue: Union[queue.LifoQueue, None]) -> AsyncAssetJob:
        """create_asset"""
        cur = self._conn.cursor()
        cur.execute(obj.get_create_statement().strip(), num_statements=0, _exec_async=True)
        return AsyncAssetJob(
            conn=self._conn,
            cursor=cur,
            query_id=cur.sfqid,
            asset=obj,
            queue=asset_queue
        )

    def delete_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        """delete_asset"""
        return self._delete_asset_async(obj)

    def delete_assets(self, asset_queue: queue.LifoQueue) -> None:
        """delete_asset"""
        while not asset_queue.empty():
            self._delete_asset_async(asset_queue.get()).wait()

    def _delete_asset_async(self, obj: ISnowflakeAsset) -> AsyncAssetJob:
        """delete_asset"""
        return AsyncDescribeJob(
            original=self._session.sql(obj.get_delete_statement()).collect_nowait(), 
            is_procedure=False,
            config=None,
        )

    def describe_async(self, describable: ISnowflakeDescribable) -> AsyncDescribeJob:
        """The ISnowflakeDescribable must contain a single statement query (1x';')"""
        return AsyncDescribeJob(
            original=self._session.sql(describable.get_describe_statement()).collect_nowait(), 
            is_procedure=describable.is_procedure(),
            config=describable.get_dacite_config(),
        )

    # def merge_into(self, obj: U) -> bool:
    #     """merge_into"""
    #     try:
    #         self._conn.execute_string(obj.merge_into_statement())
    #         return True
    #     except Exception as e:
    #         # print(e)
    #         print("merge_into threw an exception!")
    #     return False

    # def get_mergeable(self, obj: U) -> U:
    #     """get_mergeable"""
    #     module = importlib.import_module(obj.__module__)
    #     class_ = getattr(module, obj.__class__.__name__)
    #     with self._conn.cursor() as cur:
    #         cur.execute(obj.select_statement(self.gov_db, self.mgmt_schema))
    #         row = cur.fetchone()
    #         raw_data = dict(zip([c[0].lower() for c in cur.description], row))
    #         data = {}
    #         for k, v in raw_data.items():
    #             if isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
    #                 data[k] = json.loads(v)
    #             else:
    #                 data[k] = v
    #         return from_dict(data_class=class_, data=data, config=None)
