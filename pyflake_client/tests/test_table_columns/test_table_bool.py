"""test_table_bool"""
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Bool
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_table_bool(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Bool(name="BOOL_COLUMN"),
    ]
    table = TableAsset(
        schema=schema,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=AssetsRole("SYSADMIN"),
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        sf_table = flake.describe_one(
            TableDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                name=table.table_name,
            ),
            TableEntity,
        )
        ### Assert ###
        assert sf_table is not None
        assert sf_table.name == table.table_name
        assert len(sf_table.columns) == 1
        c = sf_table.columns[0]
        assert c.type == "BOOLEAN"
        assert c.name == columns[0].name
        assert c.primary_key is False
        assert c.unique_key is False
        assert c.nullable is False
        assert c.default is None
        assert c.expression is None
        assert c.check is None
        assert c.policy_name is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_table_bool_primary_key(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Bool(name="BOOL_COLUMN", primary_key=True),
    ]
    table = TableAsset(
        schema=schema,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=AssetsRole("SYSADMIN"),
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        sf_table = flake.describe_one(
            TableDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                name=table.table_name,
            ),
            TableEntity,
        )
        ### Assert ###
        assert sf_table is not None
        assert sf_table.name == table.table_name
        assert len(sf_table.columns) == 1
        c = sf_table.columns[0]
        assert c.type == "BOOLEAN"
        assert c.name == columns[0].name
        assert c.primary_key is True
        assert c.unique_key is False
        assert c.nullable is False
        assert c.default is None
        assert c.expression is None
        assert c.check is None
        assert c.policy_name is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
