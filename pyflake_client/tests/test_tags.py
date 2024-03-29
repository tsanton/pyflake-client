# -*- coding: utf-8 -*-
import queue

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.role_inheritance import (
    RoleInheritance as RoleInheritanceAsset,
)
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.assets.tag import Tag as TagAsset
from pyflake_client.models.describables.tag import Tag as TagDescribable
from pyflake_client.models.entities.tag import Tag as TagEntity


def test_describe_non_existing_tag(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    try:
        flake.register_asset_async(database, asset_queue=assets_queue).wait()
        flake.register_asset_async(schema, asset_queue=assets_queue).wait()

        ### Act ###
        tag = flake.describe_async(
            describable=TagDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name="I_DONT_EXIST_TAG",
            )
        ).deserialize_one(TagEntity)
        ### Assert ###
        assert tag is None
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)


def test_create_tag_without_tag_values(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    tag = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=[],
        owner=RoleAsset(name="SYSADMIN"),
        comment="TEST TAG",
    )
    try:
        flake.register_asset_async(database, asset_queue=assets_queue).wait()
        flake.register_asset_async(schema, asset_queue=assets_queue).wait()
        flake.register_asset_async(tag, asset_queue=assets_queue).wait()
        ### Act ###
        sf_tag = flake.describe_async(
            describable=TagDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name=tag.tag_name,
            )
        ).deserialize_one(TagEntity)
        ### Assert ###
        assert sf_tag is not None
        assert sf_tag.name == tag.tag_name
        assert sf_tag.allowed_values is None
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)


def test_create_tag_with_tag_values(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    tag = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=["FOO", "BAR"],
        owner=RoleAsset(name="SYSADMIN"),
        comment="TEST TAG",
    )
    try:
        flake.register_asset_async(database, asset_queue=assets_queue).wait()
        flake.register_asset_async(schema, asset_queue=assets_queue).wait()
        flake.register_asset_async(tag, asset_queue=assets_queue).wait()
        ### Act ###
        sf_tag = flake.describe_async(
            describable=TagDescribable(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name=tag.tag_name,
            )
        ).deserialize_one(TagEntity)
        ### Assert ###
        assert sf_tag is not None
        assert sf_tag.name == tag.tag_name
        assert sf_tag.allowed_values is not None
        assert "FOO" in sf_tag.allowed_values
        assert "BAR" in sf_tag.allowed_values
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)


def test_create_tag_with_database_role_owner(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    db_role = DatabaseRoleAsset(
        name="PYFLAKE_CLIENT_TEST_DB_ROLE",
        database_name=database.db_name,
        comment=comment,
        owner=RoleAsset("USERADMIN"),
    )
    rel = RoleInheritanceAsset(
        child_principal=db_role, parent_principal=sys_admin
    )  # So we can delete the schema in the finally
    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=db_role,
    )
    tag = TagAsset(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=[],
        comment=comment,
        owner=db_role,
    )
    try:
        flake.register_asset_async(database, asset_queue=assets_queue).wait()
        w1 = flake.register_asset_async(db_role, asset_queue=assets_queue)
        w2 = flake.register_asset_async(schema, asset_queue=assets_queue)
        flake.wait_all([w1, w2])
        w3 = flake.create_asset_async(rel)
        w4 = flake.register_asset_async(tag, asset_queue=assets_queue)
        flake.wait_all([w3, w4])

        ### Act ###
        sf_tag = flake.describe_async(
            TagDescribable(database_name=database.db_name, schema_name=schema.schema_name, tag_name=tag.tag_name)
        ).deserialize_one(TagEntity)

        ### Assert ###
        assert sf_tag is not None
        assert sf_tag.name == tag.tag_name
        assert sf_tag.allowed_values is None
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)
