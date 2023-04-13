"""pyflake_client"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import importlib
import queue
import json
from typing import Any, List, Type, TypeVar, Union

from dacite import from_dict
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from pyflake_client.models.assets.snowflake_asset_interface import ISnowflakeAsset
from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.executables.snowflake_executable_interface import (
    ISnowflakeExecutable,
)
from pyflake_client.models.mergeables.snowflake_mergable_interface import (
    ISnowflakeMergable,
)
from pyflake_client.tests.models.mergable_entity import MergableEntity


T = TypeVar("T", bound=ISnowflakeEntity)


class PyflakeClient:
    """PyflakeClient"""

    def __init__(
        self, conn: SnowflakeConnection, gov_db: str = None, mgmt_schema: str = None
    ) -> None:
        self._conn: SnowflakeConnection = conn
        self.gov_db = gov_db
        self.mgmt_schema = mgmt_schema

    def execute_scalar(self, query: str) -> Any:
        """execute_scalar"""
        with self._conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            if not row:
                return None
            return row[0]

    def execute(self, executable: ISnowflakeExecutable) -> Any:
        """execute"""
        with self._conn.cursor() as cur:
            cur.execute(executable.get_call_statement())
            data = cur.fetchall()
            if not data:
                return None
            if len(data) == 1:
                return data[0][0]
            return [x[0] for x in data]

    def create_asset(self, obj: ISnowflakeAsset) -> None:
        """create_asset"""
        self._create_asset(obj)

    def register_asset(
        self, obj: ISnowflakeAsset, asset_queue: queue.LifoQueue
    ) -> None:
        """register_asset"""
        self._create_asset(obj)
        asset_queue.put(obj)

    def _create_asset(self, obj: ISnowflakeAsset):
        """create_asset"""
        self._conn.execute_string(obj.get_create_statement())

    def delete_asset(self, obj: ISnowflakeAsset) -> None:
        """delete_asset"""
        self._delete_asset(obj)

    def delete_assets(self, asset_queue: queue.LifoQueue) -> None:
        """delete_asset"""
        while not asset_queue.empty():
            self._delete_asset(asset_queue.get())

    def _delete_asset(self, obj: ISnowflakeAsset) -> None:
        """delete_asset"""
        self._conn.execute_string(obj.get_delete_statement())

    def describe_one(
        self, describable: ISnowflakeDescribable, entity: Type[T]
    ) -> Union[T, None]:
        class_ = entity
        with self._conn.cursor() as cur:
            try:
                cur.execute(describable.get_describe_statement())
            except ProgrammingError:
                return None  # TODO : this seems unsafe

            row = cur.fetchone()
            if not row:
                return None

            if describable.is_procedure():
                data = json.loads(row[0])
                if data in ({}, []):
                    return None

                return class_.load_from_sf(data=data, config=describable.get_dacite_config())

            return class_.load_from_sf(data=dict(zip([c[0] for c in cur.description], row)),config=describable.get_dacite_config())

    def describe_many(
        self, describable: ISnowflakeDescribable, entity: Type[T]
    ) -> Union[List[T], None]:
        class_ = entity
        with self._conn.cursor() as cur:
            try:
                cur.execute(describable.get_describe_statement())
            except ProgrammingError:
                return None  # TODO : this seems unsafe

            res = cur.fetchall()
            if not res:
                return []

            if describable.is_procedure():
                data = [json.loads(r) for r in res[0]][0]
                if data in ({}, []):
                    return None

                return [
                    class_.load_from_sf(data=d, config=describable.get_dacite_config()) for d in data
                ]

            return [
                class_.load_from_sf(
                    data=dict(zip([c[0] for c in cur.description], r)),
                    config=describable.get_dacite_config(),
                )
                for r in res
            ]

    def merge_into(self, obj: ISnowflakeMergable) -> bool:
        """merge_into"""
        try:
            self._conn.execute_string(
                obj.merge_into_statement(self.gov_db, self.mgmt_schema)
            )
            return True
        except Exception as e:
            print(e)
        return False

    def get_mergeable(self, obj: ISnowflakeMergable) -> MergableEntity:
        """get_mergeable"""
        module = importlib.import_module(obj.__module__)
        class_ = getattr(module, obj.__class__.__name__)
        with self._conn.cursor() as cur:
            cur.execute(obj.select_statement(self.gov_db, self.mgmt_schema))
            row = cur.fetchone()
            raw_data = dict(zip([c[0].lower() for c in cur.description], row))
            data = {}
            for k, v in raw_data.items():
                if isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
                    data[k] = json.loads(v)
                else:
                    data[k] = v
            return from_dict(data_class=class_, data=data, config=None)
