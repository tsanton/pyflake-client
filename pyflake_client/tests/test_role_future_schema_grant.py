"""test_role_future_schema_grant"""
# pylint: disable=line-too-long
import queue
import uuid

from pyflake_client.pyflake_client import PyflakeClient
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.schema import Schema as AssetsSchema
from pyflake_client.models.assets.grant import Grant as GrantAsset
from pyflake_client.models.assets.grants.role_schema_future_grant import RoleSchemaFutureGrant
from pyflake_client.models.describables.grants.role_future_grant import RoleFutureGrant as RoleFutureGrantDescribable
from pyflake_client.models.entities.grants.role_future_grant import RoleFutureGrants as RoleFutureGrantsEntity
from pyflake_client.models.enums.object_type import ObjectType


def test_grant_role_future_schema_privilege(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_grant_role_future_schema_privilege"""
    ### Arrange ###
    database = AssetsDatabase("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}")
    schema = AssetsSchema(database=database, schema_name="IGT_SCHEMA", comment=f"pyflake_client_TEST_{uuid.uuid4()}")
    role = AssetsRole("IGT_CREATE_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    privilege = GrantAsset(RoleSchemaFutureGrant(role.name, database.db_name, schema.schema_name, ObjectType.TABLE), ["SELECT"])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(privilege, assets_queue)

        ### Act ###
        granted: RoleFutureGrantsEntity = flake.describe(RoleFutureGrantDescribable(role.name), RoleFutureGrantsEntity)

        ### Assert ###
        assert granted.role_name == role.name
        assert len(granted.grants) == 1
        priv = next((r for r in granted.grants if r.privilege == "SELECT"), None)
        assert priv is not None
        assert priv.name == f"{database.db_name}.{schema.schema_name}.<{ObjectType.TABLE}>"
        assert priv.grant_on == ObjectType.TABLE

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_grant_role_schema_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_grant_role_schema_privileges"""
    ### Arrange ###
    database: AssetsDatabase = AssetsDatabase("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}")
    schema = AssetsSchema(database=database, schema_name="IGT_SCHEMA", comment=f"pyflake_client_TEST_{uuid.uuid4()}")
    role = AssetsRole("IGT_CREATE_ROLE", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    p1 = GrantAsset(RoleSchemaFutureGrant(role.name, database.db_name, schema.schema_name, ObjectType.TABLE), ["SELECT", "UPDATE"])
    p2 = GrantAsset(RoleSchemaFutureGrant(role.name, database.db_name, schema.schema_name, ObjectType.VIEW),  ["SELECT", "REFERENCES"])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(p1, assets_queue)
        flake.register_asset(p2, assets_queue)

        ### Act ###
        granted: RoleFutureGrantsEntity = flake.describe(RoleFutureGrantDescribable(role.name), RoleFutureGrantsEntity)

        ### Assert ###
        assert granted.role_name == role.name
        assert len(granted.grants) == 4

        table_schema_scope = f"{database.db_name}.{schema.schema_name}.<{ObjectType.TABLE}>"
        found = next((r for r in granted.grants if r.privilege == "SELECT" and r.name == table_schema_scope), None)
        assert found is not None

        found = next((r for r in granted.grants if r.privilege == "UPDATE" and r.name == table_schema_scope), None)
        assert found is not None

        view_schema_scope = f"{database.db_name}.{schema.schema_name}.<{ObjectType.VIEW}>"
        found = next((r for r in granted.grants if r.privilege == "SELECT" and r.name == view_schema_scope), None)
        assert found is not None

        found = next((r for r in granted.grants if r.privilege == "REFERENCES" and r.name == view_schema_scope), None)
        assert found is not None

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
