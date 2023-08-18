# -*- coding: utf-8 -*-
import queue
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Identity, Number, Varchar
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.entities.table import Table as TableEntity


def test_create_table_with_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]
    table = TableAsset(
        db_name=database.db_name,
        schema_name=schema.schema_name,
        table_name="TEST",
        columns=columns,
        owner=RoleAsset("SYSADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(table, assets_queue).wait()

        ### Act ###
        t = flake.describe_async(
            TableDescribable(database.db_name, schema.schema_name, table.table_name),
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.database_name == database.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "SYSADMIN"
        assert t.retention_time == 1
        assert t.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_without_owner(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]
    table = TableAsset(db_name=database.db_name, schema_name=schema.schema_name, table_name="TEST", columns=columns)

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(table, assets_queue).wait()

        ### Act ###
        t = flake.describe_async(
            TableDescribable(database.db_name, schema.schema_name, table.table_name)
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.database_name == database.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "ACCOUNTADMIN"
        assert t.retention_time == 1
        assert t.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
