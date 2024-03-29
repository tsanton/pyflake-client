# -*- coding: utf-8 -*-
import queue

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import (
    ClassificationTag,
    Identity,
    Number,
    Varchar,
)
from pyflake_client.models.assets.tag import Tag as TagAsset
from pyflake_client.models.describables.table import Table as TableDescribable
from pyflake_client.models.entities.table import Table as TableEntity


def test_create_column_with_tag_without_value(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    sysadmin = RoleAsset("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_GOVERNANCE_DB_{rand_str}", comment, owner=sysadmin)
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=sysadmin,
    )

    tag_asset = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=[],
        owner=sysadmin,
    )
    classification_tag = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset.tag_name,
        tag_value=None,
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True, tags=[classification_tag]),
    ]
    table = TableAsset(
        db_name=database.db_name,
        schema_name=schema.schema_name,
        table_name="TEST",
        columns=columns,
        owner=sysadmin,
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        w1 = flake.register_asset_async(tag_asset, assets_queue)
        w2 = flake.register_asset_async(table, assets_queue)
        flake.wait_all([w1, w2])

        ### Act ###
        t = flake.describe_async(
            TableDescribable(database.db_name, schema.schema_name, table.table_name),
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.columns is not None
        assert len(t.columns) == 2
        tagged_col = next(c for c in t.columns if c.name == "SOME_VARCHAR")
        assert tagged_col is not None
        assert tagged_col.tags is not None
        assert len(tagged_col.tags) == 1
        tag = tagged_col.tags[0]
        assert tag.tag_name == tag_asset.tag_name
        assert tag.tag_value is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_column_with_tag_with_value(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    sysadmin = RoleAsset("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=sysadmin)
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=sysadmin,
    )
    tag_asset = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=["FOO", "BAR"],
        owner=sysadmin,
    )
    classification_tag = ClassificationTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name=tag_asset.tag_name,
        tag_value="FOO",
    )
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar("SOME_VARCHAR", primary_key=True, tags=[classification_tag]),
    ]
    table = TableAsset(
        db_name=database.db_name, schema_name=schema.schema_name, table_name="TEST", columns=columns, owner=sysadmin
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        w1 = flake.register_asset_async(tag_asset, assets_queue)
        w2 = flake.register_asset_async(table, assets_queue)
        flake.wait_all([w1, w2])

        ### Act ###
        t = flake.describe_async(
            TableDescribable(database.db_name, schema.schema_name, table.table_name)
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.columns is not None
        assert len(t.columns) == 2
        tagged_col = next(c for c in t.columns if c.name == "SOME_VARCHAR")
        assert tagged_col is not None
        assert tagged_col.tags is not None
        assert len(tagged_col.tags) == 1
        tag = tagged_col.tags[0]
        assert tag.tag_name == tag_asset.tag_name
        assert tag.tag_value == "FOO"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_with_multiple_tags(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    sysadmin = RoleAsset("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=sysadmin)
    schema = Schema(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=sysadmin,
    )
    tag_asset_1 = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG_1",
        tag_values=[],
        owner=sysadmin,
    )
    tag_asset_2 = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG_2",
        tag_values=["FOO", "BAR"],
        owner=sysadmin,
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
    columns = [
        Number("ID", identity=Identity(1, 1)),
        Varchar(
            "SOME_VARCHAR",
            primary_key=True,
            tags=[classification_tag_1, classification_tag_2],
        ),
    ]
    table = TableAsset(
        db_name=database.db_name, schema_name=schema.schema_name, table_name="TEST", columns=columns, owner=sysadmin
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(schema, assets_queue).wait()
        w1 = flake.register_asset_async(tag_asset_1, assets_queue)
        w2 = flake.register_asset_async(tag_asset_2, assets_queue)
        w3 = flake.register_asset_async(table, assets_queue)
        flake.wait_all([w1, w2, w3])

        ### Act ###
        t = flake.describe_async(
            TableDescribable(database.db_name, schema.schema_name, table.table_name)
        ).deserialize_one(TableEntity)

        ### Assert ###
        assert t is not None
        assert t.name == table.table_name
        assert t.columns is not None
        assert len(t.columns) == 2
        tagged_col = next(c for c in t.columns if c.name == "SOME_VARCHAR")
        assert tagged_col is not None
        assert tagged_col.tags is not None
        assert len(tagged_col.tags) == 2
        tag_1 = next(t for t in tagged_col.tags if t.tag_name == tag_asset_1.tag_name)
        assert tag_1 is not None
        assert tag_1.tag_value is None
        assert tag_1.tag_name == tag_asset_1.tag_name
        tag_2 = next(t for t in tagged_col.tags if t.tag_name == tag_asset_2.tag_name)
        assert tag_2 is not None
        assert tag_2.tag_value == "FOO"
        assert tag_2.tag_name == tag_asset_2.tag_name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
