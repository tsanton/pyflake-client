# -*- coding: utf-8 -*-
import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import Variant
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.entities.column import Variant as VariantEntity
from pyflake_client.models.entities.table import Table as TableEntity


def test_table_variant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Variant(name="VARIANT_COLUMN"),
    ]
    table = TableAsset(
        db_name=database.db_name,
        schema_name=schema.schema_name,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=RoleAsset("SYSADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(table, assets_queue).wait()

        ### Act ###
        sf_table = flake.describe_async(
            TableDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                name=table.table_name,
            )
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert sf_table is not None
        assert sf_table.name == table.table_name
        assert len(sf_table.columns) == 1
        c = sf_table.columns[0]
        assert isinstance(c, VariantEntity)
        assert c.type == "VARIANT"
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


def test_table_variant_primary_key(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Variant(name="VARIANT_COLUMN", primary_key=True),
    ]
    table = TableAsset(
        db_name=database.db_name,
        schema_name=schema.schema_name,
        table_name="TEST_TABLE",
        columns=columns,  # type: ignore
        owner=RoleAsset("SYSADMIN"),
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(table, assets_queue).wait()

        ### Act ###
        sf_table = flake.describe_async(
            TableDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                name=table.table_name,
            )
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert sf_table is not None
        assert sf_table.name == table.table_name
        assert len(sf_table.columns) == 1
        c = sf_table.columns[0]
        assert isinstance(c, VariantEntity)
        assert c.type == "VARIANT"
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
