# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
import queue
from datetime import date

import pytest

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.database import Database as DatabaseDescribable
from pyflake_client.models.entities.database import Database as DatabaseEntity


def test_get_database(flake: PyflakeClient):
    ### Act ###
    database = flake.describe_async(DatabaseDescribable("SNOWFLAKE")).deserialize_one(DatabaseEntity)

    ### Assert ###
    assert database is not None
    assert database.name == "SNOWFLAKE"
    assert database.origin == "SNOWFLAKE.ACCOUNT_USAGE"
    assert database.retention_time == 0


def test_get_database_that_does_not_exist(flake: PyflakeClient):
    ### Act ###
    database = flake.describe_async(DatabaseDescribable("I_DO_NOT_EXIST")).deserialize_one(DatabaseEntity)

    ### Assert ###
    assert database is None


def test_create_database_with_role_owner(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    database: DatabaseAsset = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))

    try:
        flake.register_asset_async(database, assets_queue).wait()

        ### Act ###
        db = flake.describe_async(DatabaseDescribable(database.db_name)).deserialize_one(DatabaseEntity)
        ### Assert ###
        assert db is not None
        assert db.name == database.db_name
        assert db.comment == database.comment
        assert db.owner == "SYSADMIN"
        assert db.retention_time == 1
        assert db.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_database_with_database_role_owner(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    with pytest.raises(NotImplementedError):
        flake.register_asset_async(
            DatabaseAsset(
                f"PYFLAKE_CLIENT_TEST_DB_{rand_str}",
                comment,
                owner=DatabaseRoleAsset(f"PYFLAKE_CLIENT_TEST_DB_ROLE_{rand_str}", "CANT_OWN_DATABASES"),
            ),
            assets_queue,
        ).wait()
