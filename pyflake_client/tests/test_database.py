"""test_database"""
# pylint: disable=line-too-long
import queue
import uuid
from datetime import date

import pytest

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.entities.database import Database as DatabaseEntity
from pyflake_client.models.describables.database import Database as DatabaseDescribable

from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset

def test_get_database(flake: PyflakeClient):
    """test_get_database"""
    ### Act ###
    database = flake.describe_one(DatabaseDescribable("SNOWFLAKE"), DatabaseEntity)

    ### Assert ###
    assert database is not None
    assert database.name == "SNOWFLAKE"
    assert database.origin == "SNOWFLAKE.ACCOUNT_USAGE"


def test_get_database_that_does_not_exist(flake: PyflakeClient):
    """test_get_database_does_not_exist"""
    ### Act ###
    database = flake.describe_one(DatabaseDescribable("I_DO_NOT_EXIST"), DatabaseEntity)

    ### Assert ###
    assert database is None

def test_create_database_with_role_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_database"""

    ### Arrange ###
    database: DatabaseAsset = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))

    try:
        flake.register_asset(database, assets_queue)

        ### Act ###
        sf_db = flake.describe_one(DatabaseDescribable(database.db_name), DatabaseEntity)
        ### Assert ###
        assert sf_db is not None
        assert sf_db.name == database.db_name
        assert sf_db.comment == database.comment
        assert sf_db.owner == "SYSADMIN"
        assert sf_db.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_database_with_database_role_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_database_with_database_role_owner"""
    with pytest.raises(NotImplementedError):
        flake.register_asset(DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=DatabaseRoleAsset("DATABASE_ROLE", "CANT_OWN_DATABASES")), assets_queue)