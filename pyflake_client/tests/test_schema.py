"""test_schema"""
# pylint: disable=line-too-long
import queue
import uuid
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.schema import Schema as AssetsSchema
from pyflake_client.models.entities.schema import Schema as EntitiesSchema
from pyflake_client.models.describables.schema import Schema as DescribabablesSchema


def test_create_schema(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_schema"""
    ### Arrange ###
    database = AssetsDatabase("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}")
    some_schema = AssetsSchema(database=database, schema_name="SOME_SCHEMA", comment=f"pyflake_client_TEST_{uuid.uuid4()}")
    another_schema = AssetsSchema(database=database, schema_name="ANOTHER_SCHEMA", comment=f"pyflake_client_TEST_{uuid.uuid4()}")

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(some_schema, assets_queue)
        flake.register_asset(another_schema, assets_queue)

        ### Act ###
        sch_so: EntitiesSchema = flake.describe(DescribabablesSchema(
            some_schema.schema_name, some_schema.database.db_name), EntitiesSchema)
        sch_an: EntitiesSchema = flake.describe(DescribabablesSchema(
            another_schema.schema_name, another_schema.database.db_name), EntitiesSchema)

        ### Assert ###
        assert sch_so.name == some_schema.schema_name
        assert sch_so.database_name == some_schema.database.db_name
        assert sch_so.comment == some_schema.comment
        assert sch_so.owner == "SYSADMIN"
        assert sch_so.created_on.date() == date.today()

        assert sch_an.name == another_schema.schema_name
        assert sch_an.database_name == another_schema.database.db_name
        assert sch_an.comment == another_schema.comment
        assert sch_an.owner == "SYSADMIN"
        assert sch_an.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_schema(flake: PyflakeClient):
    """test_get_schema"""
    ### Act ###
    schema: EntitiesSchema = flake.describe(DescribabablesSchema("INFORMATION_SCHEMA", "SNOWFLAKE"), EntitiesSchema)

    ### Assert ###
    assert schema.name == "INFORMATION_SCHEMA"
    assert schema.database_name == "SNOWFLAKE"


def test_get_schema_when_database_does_not_exist(flake: PyflakeClient):
    """test_get_schema_when_database_does_not_exist"""
    ### Act ###
    schema: EntitiesSchema = flake.describe(DescribabablesSchema("INFORMATION_SCHEMA", "THIS_DB_DOES_NOT_EXIST"), EntitiesSchema)

    ### Assert ###
    assert schema is None


def test_get_schema_when_schema_does_not_exist(flake: PyflakeClient):
    """test_get_schema_when_schema_does_not_exist"""
    ### Act ###
    schema: EntitiesSchema = flake.describe(DescribabablesSchema("THIS_SCHEMA_DOES_NOT_EXIST", "SNOWFLAKE"), EntitiesSchema)

    ### Assert ###
    assert schema is None
