"""test_database"""
# pylint: disable=line-too-long
import queue
import uuid
from datetime import date

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.entities.database import Database as EntitiesDatabase
from pyflake_client.models.describables.database import Database as DescribablesDatabase


def test_create_database(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_database"""
    ### Arrange ###
    database: AssetsDatabase = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}"
    )

    try:
        flake.register_asset(database, assets_queue)

        ### Act ###
        sf_db: EntitiesDatabase = flake.describe(DescribablesDatabase(database.db_name), EntitiesDatabase)
        ### Assert ###
        assert sf_db.name == database.db_name
        assert sf_db.comment == database.comment
        assert sf_db.owner == "SYSADMIN"
        assert sf_db.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_database(flake: PyflakeClient):
    """test_get_database"""
    ### Act ###
    database: EntitiesDatabase = flake.describe(DescribablesDatabase("SNOWFLAKE"), EntitiesDatabase)

    ### Assert ###
    assert database.name == "SNOWFLAKE"
    assert database.origin == "SNOWFLAKE.ACCOUNT_USAGE"


def test_get_database_that_does_not_exist(flake: PyflakeClient):
    """test_get_database_does_not_exist"""
    ### Act ###
    database: EntitiesDatabase = flake.describe(DescribablesDatabase("I_DO_NOT_EXIST"), EntitiesDatabase)

    ### Assert ###
    assert database is None
