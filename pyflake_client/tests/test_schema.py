"""test_schema"""
import queue
import uuid
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.entities.schema import Schema as SchemaEntity
from pyflake_client.models.describables.schema import Schema as SchemaDescribable


def test_create_schema(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_schema"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    some_schema = SchemaAsset(
        database=database,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    another_schema = SchemaAsset(
        database=database,
        schema_name="ANOTHER_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(some_schema, assets_queue)
        flake.register_asset(another_schema, assets_queue)

        ### Act ###
        sch_so = flake.describe_one(
            SchemaDescribable(some_schema.schema_name, some_schema.database.db_name),
            SchemaEntity,
        )
        sch_an = flake.describe_one(
            SchemaDescribable(
                another_schema.schema_name, another_schema.database.db_name
            ),
            SchemaEntity,
        )

        ### Assert ###
        assert sch_so is not None
        assert sch_so.name == some_schema.schema_name
        assert sch_so.database_name == some_schema.database.db_name
        assert sch_so.comment == some_schema.comment
        assert sch_so.owner == "SYSADMIN"
        assert sch_so.created_on.date() == date.today()

        assert sch_an is not None
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
    schema = flake.describe_one(
        SchemaDescribable("INFORMATION_SCHEMA", "SNOWFLAKE"), SchemaEntity
    )

    ### Assert ###
    assert schema is not None
    assert schema.name == "INFORMATION_SCHEMA"
    assert schema.database_name == "SNOWFLAKE"


def test_get_schema_when_database_does_not_exist(flake: PyflakeClient):
    """test_get_schema_when_database_does_not_exist"""
    ### Act ###
    schema = flake.describe_one(
        SchemaDescribable("INFORMATION_SCHEMA", "THIS_DB_DOES_NOT_EXIST"),
        SchemaEntity,
    )

    ### Assert ###
    assert schema is None


def test_get_schema_when_schema_does_not_exist(flake: PyflakeClient):
    """test_get_schema_when_schema_does_not_exist"""
    ### Act ###
    schema = flake.describe_one(
        SchemaDescribable("THIS_SCHEMA_DOES_NOT_EXIST", "SNOWFLAKE"), SchemaEntity
    )

    ### Assert ###
    assert schema is None
