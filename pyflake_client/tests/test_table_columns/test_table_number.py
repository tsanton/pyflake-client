"""test_table_number"""
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Number
from pyflake_client.models.entities.column import Number as NumberEntity
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_table_number(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, db_asset_fixture: Database
):
    schema = Schema(
        database=db_asset_fixture,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Number(name="NUMBER_COLUMN"),
    ]
    table = TableAsset(
        schema=schema,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=AssetsRole("SYSADMIN"),
    )

    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        sf_table = flake.describe_one(
            TableDescribable(
                database_name=db_asset_fixture.db_name,
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
        assert isinstance(c, NumberEntity)
        assert c.type == "NUMBER(38,37)"
        assert c.scale == 37
        assert c.precision == 38
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


def test_table_number_primary_key(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, db_asset_fixture: Database
):
    schema = Schema(
        database=db_asset_fixture,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Number(name="NUMBER_COLUMN", primary_key=True),
    ]
    table = TableAsset(
        schema=schema,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=AssetsRole("SYSADMIN"),
    )

    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        sf_table = flake.describe_one(
            TableDescribable(
                database_name=db_asset_fixture.db_name,
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
        assert isinstance(c, NumberEntity)
        assert c.type == "NUMBER(38,37)"
        assert c.scale == 37
        assert c.precision == 38
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
