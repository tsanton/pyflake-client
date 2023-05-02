"""test_table"""
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Number, Varchar, Identity
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_create_table_with_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema: Schema = Schema(
        db_name=database.db_name,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]
    table = TableAsset(db_name=database.db_name, schema_name=schema.schema_name, table_name="TEST", columns=columns, owner=RoleAsset("SYSADMIN"))

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        t = flake.describe_one(
            TableDescribable(database.db_name, schema.schema_name, table.table_name),
            TableEntity,
        )

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.database_name == database.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "SYSADMIN"
        assert t.retention_time == "1"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_without_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema: Schema = Schema(
        db_name=database.db_name,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]
    table = TableAsset(db_name=database.db_name, schema_name=schema.schema_name, table_name="TEST", columns=columns)

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        t = flake.describe_one(
            TableDescribable(database.db_name, schema.schema_name, table.table_name),
            TableEntity,
        )

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.database_name == database.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "ACCOUNTADMIN"
        assert t.retention_time == "1"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
