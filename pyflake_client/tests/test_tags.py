import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.schema import Schema as AssetsSchema
from pyflake_client.models.assets.tag import Tag as AssetsTag
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.describables.tag import Tag as DescribablesTag
from pyflake_client.models.entities.tag import Tag as EntitiesTag


def test_describe_non_existing_tag(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = AssetsSchema(
        database=database,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    try:
        flake.register_asset(database, asset_queue=assets_queue)
        flake.register_asset(schema, asset_queue=assets_queue)

        ### Act ###
        tag = flake.describe_one(
            describable=DescribablesTag(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name="I_DONT_EXIST_TAG",
            ),
            entity=EntitiesTag,
        )
        ### Assert ###
        assert tag is None
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)


def test_create_tag_without_tag_values(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = AssetsSchema(
        database=database,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    tag = AssetsTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=[],
        owner=AssetsRole(name="SYSADMIN"),
        comment="TEST TAG",
    )
    try:
        flake.register_asset(database, asset_queue=assets_queue)
        flake.register_asset(schema, asset_queue=assets_queue)
        flake.register_asset(tag, asset_queue=assets_queue)
        ### Act ###
        sf_tag = flake.describe_one(
            describable=DescribablesTag(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name=tag.tag_name,
            ),
            entity=EntitiesTag,
        )
        ### Assert ###
        assert sf_tag is not None
        assert sf_tag.name == tag.tag_name
        assert sf_tag.allowed_values is None
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)


def test_create_tag_with_tag_values(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = AssetsSchema(
        database=database,
        schema_name="TEST_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    tag = AssetsTag(
        database_name=database.db_name,
        schema_name=schema.schema_name,
        tag_name="TEST_TAG",
        tag_values=["FOO", "BAR"],
        owner=AssetsRole(name="SYSADMIN"),
        comment="TEST TAG",
    )
    try:
        flake.register_asset(database, asset_queue=assets_queue)
        flake.register_asset(schema, asset_queue=assets_queue)
        flake.register_asset(tag, asset_queue=assets_queue)
        ### Act ###
        sf_tag = flake.describe_one(
            describable=DescribablesTag(
                database_name=database.db_name,
                schema_name=schema.schema_name,
                tag_name=tag.tag_name,
            ),
            entity=EntitiesTag,
        )
        ### Assert ###
        assert sf_tag is not None
        assert sf_tag.name == tag.tag_name
        assert sf_tag.allowed_values is not None
        assert "FOO" in sf_tag.allowed_values
        assert "BAR" in sf_tag.allowed_values
    finally:
        ### Cleanup ###
        flake.delete_assets(asset_queue=assets_queue)
