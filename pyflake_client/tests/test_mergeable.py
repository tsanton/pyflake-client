# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals


import queue
from datetime import date
from typing import Tuple

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Table
from pyflake_client.models.describables.queryable import Queryable
from pyflake_client.tests.models.mergable_entity import (
    TABLE_COLUMN_DEFINITION,
    TABLE_NAME,
    MergableEntity,
)


def test_merge_into(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    ### Arrange ###
    db, s, _, _, _ = proc_db
    t = Table(db.db_name, s.schema_name, TABLE_NAME, TABLE_COLUMN_DEFINITION)

    try:
        flake.register_asset_async(t, assets_queue).wait()

        ### Act ###
        ins = MergableEntity("TEST", True).configure(db.db_name, s.schema_name, TABLE_NAME)
        flake.execute_async(ins.merge_into_statement()).wait()
        entity: MergableEntity = flake.describe_async(Queryable(ins.select_statement())).deserialize_one(
            MergableEntity, MergableEntity.get_deserializer()
        )

        ### Assert ###
        assert entity is not None
        assert entity.the_primary_key == ins.the_primary_key
        assert entity.enabled == ins.enabled
        assert entity.id == 1
        assert entity.valid_from is not None
        assert entity.valid_to is not None
        assert entity.valid_from.date() == date.today()
        assert entity.valid_to.date() == date(9999, 12, 31)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_merge_into_and_update(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    ### Arrange ###
    db, s, _, _, _ = proc_db
    t = Table(db.db_name, s.schema_name, TABLE_NAME, TABLE_COLUMN_DEFINITION)

    try:
        flake.register_asset_async(t, assets_queue).wait()

        ### Act ###
        ins = MergableEntity("TEST", True).configure(db.db_name, s.schema_name, TABLE_NAME)
        flake.execute_async(ins.merge_into_statement()).wait()
        ins.enabled = False
        flake.execute_async(ins.merge_into_statement()).wait()
        entity: MergableEntity = flake.describe_async(Queryable(ins.select_statement())).deserialize_one(
            MergableEntity, MergableEntity.get_deserializer()
        )
        inserted = flake.execute_async(f"select count(1) from {db.db_name}.{s.schema_name}.{TABLE_NAME}").fetch_one(
            int, lambda x: int(x)
        )

        ### Assert ###
        assert inserted == 2
        assert entity.the_primary_key == ins.the_primary_key
        assert entity.enabled == ins.enabled
        assert entity.id == 2
        assert entity.valid_from is not None
        assert entity.valid_to is not None
        assert entity.valid_from.date() == date.today()
        assert entity.valid_to.date() == date(9999, 12, 31)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_merge_into_and_delete(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    ### Arrange ###
    db, s, _, _, _ = proc_db
    t = Table(db.db_name, s.schema_name, TABLE_NAME, TABLE_COLUMN_DEFINITION)

    try:
        flake.register_asset_async(t, assets_queue).wait()

        ### Act ###
        ins = MergableEntity("TEST", True).configure(db.db_name, s.schema_name, TABLE_NAME)
        flake.execute_async(ins.merge_into_statement()).wait()
        inserted = flake.execute_async(f"select count(1) from {db.db_name}.{s.schema_name}.{TABLE_NAME}").fetch_one(
            int, lambda x: int(x)
        )
        flake.execute_async(f"delete from {db.db_name}.{s.schema_name}.{TABLE_NAME}").wait()
        deleted = flake.execute_async(f"select count(1) from {db.db_name}.{s.schema_name}.{TABLE_NAME}").fetch_one(
            int, lambda x: int(x) if x != None else 0
        )

        ### Assert ###
        assert inserted == 1
        assert deleted == 0
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
