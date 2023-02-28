"""test_table"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue
import uuid

from pyflake_client.models.assets.table import Column, Table as TableAsset
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_create_table(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database: Database = Database("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}")
    schema: Schema = Schema(database=database, schema_name="SOME_SCHEMA", comment=f"pyflake_client_TEST_{uuid.uuid4()}")
    table = TableAsset(schema, "TEST", [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(table, assets_queue)

        ### Act ###
        t: TableEntity = flake.describe(TableDescribable(database.db_name, schema.schema_name, table.table_name), TableEntity)

        ### Assert ###
        assert t.name == table.table_name
        assert t.database_name == database.db_name
        assert t.schema_name == schema.schema_name
        assert t.kind == "TABLE"
        assert t.owner == "ACCOUNTADMIN"
        assert t.retention_time == "1"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
