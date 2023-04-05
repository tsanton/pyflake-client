"""test_role_schema_grant"""


import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.schema import Schema as AssetsSchema
from pyflake_client.models.assets.grant import Grant as AssetsGrant
from pyflake_client.models.assets.grants.role_schema_grant import RoleSchemaGrant
from pyflake_client.models.describables.grant import Grant as DescribableGrant
from pyflake_client.models.entities.grant import Grant as EntitiesGrant
from pyflake_client.models.describables.schema import Schema as DescribableSchema


def test_grant_role_schema_privilege(
    flake: PyflakeClient,
    assets_queue: queue.LifoQueue,
):
    """test_grant_role_schema_privilege"""
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = AssetsSchema(
        database=database,
        schema_name="IGT_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    role = AssetsRole(
        "IGT_CREATE_ROLE",
        AssetsRole("USERADMIN"),
        f"pyflake_client_TEST_{uuid.uuid4()}",
    )
    privilege = AssetsGrant(
        RoleSchemaGrant(role.name, database.db_name, schema.schema_name),
        ["USAGE"],
    )
    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###

        grants = flake.describe_many(
            describable=DescribableGrant(
                principal=DescribableSchema(
                    name=schema.schema_name, database_name=database.db_name
                )
            ),
            entity=EntitiesGrant,
        )

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2
        priv = next((g for g in grants if g.privilege == "USAGE"), None)
        assert priv is not None
        assert priv.granted_on == "SCHEMA"
        assert priv.granted_by == "SYSADMIN"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_grant_role_schema_privileges(
    flake: PyflakeClient,
    assets_queue: queue.LifoQueue,
):
    """test_grant_role_schema_privileges"""
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = AssetsSchema(
        database=database,
        schema_name="IGT_SCHEMA",
        comment=f"pyflake_client_TEST_{uuid.uuid4()}",
        owner=AssetsRole("SYSADMIN"),
    )
    role = AssetsRole(
        "IGT_CREATE_ROLE",
        AssetsRole("USERADMIN"),
        f"pyflake_client_TEST_{uuid.uuid4()}",
    )
    privilege = AssetsGrant(
        RoleSchemaGrant(role.name, database.db_name, schema.schema_name),
        ["USAGE", "MONITOR"],
    )

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        grants = flake.describe_many(
            describable=DescribableGrant(
                principal=DescribableSchema(
                    name=schema.schema_name, database_name=database.db_name
                )
            ),
            entity=EntitiesGrant,
        )
        ### Assert ###
        assert grants is not None
        assert len(grants) == 3

        priv_usage = next((g for g in grants if g.privilege == "USAGE"), None)
        assert priv_usage is not None
        assert priv_usage.granted_on == "SCHEMA"
        assert priv_usage.granted_by == "SYSADMIN"

        priv_mon = next((g for g in grants if g.privilege == "MONITOR"), None)
        assert priv_mon is not None
        assert priv_mon.granted_on == "SCHEMA"
        assert priv_mon.granted_by == "SYSADMIN"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
