"""test_table"""
import queue
import uuid

from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import (
    Number,
    Varchar,
    Identity,
    ClassificationTag,
)
from pyflake_client.models.entities.table import Table as TableEntity
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.tag import Tag as TagAsset
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.client import PyflakeClient


def test_create_table_with_tag_without_value(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema: Schema = Schema(
        database=database,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]

    tag_asset = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=[],
        owner=RoleAsset(name="SYSADMIN"),
    )
    classification_tag = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset.tag_name,
        tag_value=None,
    )
    table = TableAsset(
        schema=schema,
        table_name="TEST",
        columns=columns,
        owner=RoleAsset("SYSADMIN"),
        tags=[classification_tag],
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(tag_asset, assets_queue)
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
        assert t.tags is not None
        assert len(t.tags) == 1
        tag = t.tags[0]
        assert tag.tag_value == None
        assert tag.tag_name == tag_asset.tag_name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_with_tag_with_value(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema: Schema = Schema(
        database=database,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]

    tag_asset = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=["FOO", "BAR"],
        owner=RoleAsset(name="SYSADMIN"),
    )
    classification_tag = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset.tag_name,
        tag_value="FOO",
    )
    table = TableAsset(
        schema=schema,
        table_name="TEST",
        columns=columns,
        owner=RoleAsset("SYSADMIN"),
        tags=[classification_tag],
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(tag_asset, assets_queue)
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
        assert t.tags is not None
        assert len(t.tags) == 1
        tag = t.tags[0]
        assert tag.tag_value == "FOO"
        assert tag.tag_name == tag_asset.tag_name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_with_multiple_tags(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_table"""
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema: Schema = Schema(
        database=database,
        schema_name="SOME_SCHEMA",
        comment=f"pyflake_client_test_{uuid.uuid4()}",
        owner=RoleAsset("SYSADMIN"),
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True),
    ]

    tag_asset_1 = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG_1",
        tag_values=[],
        owner=RoleAsset(name="SYSADMIN"),
    )
    tag_asset_2 = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG_2",
        tag_values=["FOO", "BAR"],
        owner=RoleAsset(name="SYSADMIN"),
    )
    classification_tag_1 = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset_1.tag_name,
        tag_value=None,
    )
    classification_tag_2 = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset_2.tag_name,
        tag_value="FOO",
    )
    table = TableAsset(
        schema=schema,
        table_name="TEST",
        columns=columns,
        owner=RoleAsset("SYSADMIN"),
        tags=[classification_tag_1, classification_tag_2],
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(tag_asset_1, assets_queue)
        flake.register_asset(tag_asset_2, assets_queue)
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
        assert t.tags is not None
        assert len(t.tags) == 2
        tag_1 = next(t for t in t.tags if t.tag_name == tag_asset_1.tag_name)
        assert tag_1.tag_value is None
        assert tag_1.tag_name == tag_asset_1.tag_name
        tag_2 = next(t for t in t.tags if t.tag_name == tag_asset_2.tag_name)
        assert tag_2.tag_value == "FOO"
        assert tag_2.tag_name == tag_asset_2.tag_name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
