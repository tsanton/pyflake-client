"""test_table"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Number, Varchar, Identity
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_create_table(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, db_asset_fixture: Database
):
    """test_create_table"""
    ### Arrange ###
    schema: Schema = Schema(
        database=db_asset_fixture,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]
    table = TableAsset(
        schema=schema, table_name="TEST", columns=columns, owner=AssetsRole("SYSADMIN")
    )

    try:
        flake.register_asset(db_asset_fixture, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        t = flake.describe(
            TableDescribable(
                db_asset_fixture.db_name, schema.schema_name, table.table_name
            ),
            TableEntity,
        )

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.database_name == db_asset_fixture.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "ACCOUNTADMIN"
        assert t.retention_time == "1"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
