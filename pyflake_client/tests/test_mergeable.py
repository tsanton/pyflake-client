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
    """test_merge_into"""
    ### Arrange ###
    db, s, _, _, _ = proc_db
    t = Table(db.db_name, s.schema_name, TABLE_NAME, TABLE_COLUMN_DEFINITION)

    try:
        flake.register_asset(t, assets_queue)

        ### Act ###
        ins = MergableEntity("TEST", True)
        success = flake.merge_into(ins)
        entity = flake.get_mergeable(MergableEntity(the_primary_key=ins.the_primary_key))

        ### Assert ###
        assert success is True
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


def test_merge_into_and_update(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_merge_into_and_update"""
    ### Arrange ###
    db, s, _, _, _ = _spawn_with_rwc_privileges(flake, assets_queue)
    t = Table(db.db_name, s.schema_name, TABLE_NAME, TABLE_COLUMN_DEFINITION)

    try:
        flake.register_asset(t, assets_queue)

        ### Act ###
        ins_create = MergableEntity("TEST", True)
        ins_update = MergableEntity("TEST", False)
        success_create = flake.merge_into(ins_create)
        success_update = flake.merge_into(ins_update)
        entity = flake.get_mergeable(MergableEntity(the_primary_key=ins_update.the_primary_key))
        inserted = flake.execute_scalar(f"select count(1) from {flake.gov_db}.{flake.mgmt_schema}.{TABLE_NAME}")

        ### Assert ###
        assert success_create is True
        assert success_update is True
        assert inserted == 2
        assert entity.the_primary_key == ins_update.the_primary_key
        assert entity.enabled == ins_update.enabled
        assert entity.id == 2
        assert entity.valid_from is not None
        assert entity.valid_to is not None
        assert entity.valid_from.date() == date.today()
        assert entity.valid_to.date() == date(9999, 12, 31)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
