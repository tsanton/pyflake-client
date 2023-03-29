"""test_table_bool"""
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Integer, Number, Bool, Identity
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_table_bool(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, db_asset_fixture: Database
):
    schema = Schema(
        database=db_asset_fixture,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Integer(name="ID", identity=Identity(1, 1), primary_key=True, unique=True),
        Bool(name="BOOL_COLUMN", nullable=True),
    ]
    table = TableAsset(
        schema=schema,
        table_name="TEST_TABLE",
        columns=columns,
        owner=AssetsRole("SYSADMIN"),
    )

    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        sf_table = flake.describe(
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
        assert sf_table
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
