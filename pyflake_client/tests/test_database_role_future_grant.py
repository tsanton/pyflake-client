"""test_database_role_future_grant"""
import queue
import uuid

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.grants.schema_grant import SchemaGrant
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.grants.database_object_future_grant import DatabaseObjectFutureGrant
from pyflake_client.models.assets.grants.schema_object_future_grant import SchemaObjectFutureGrant
from pyflake_client.models.assets.grant_action import GrantAction

from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable
from pyflake_client.models.describables.future_grant import FutureGrant as FutureGrantDescribable

from pyflake_client.models.entities.future_grant import FutureGrant as FutureGrantEntity

from pyflake_client.models.enums.privilege import Privilege
from pyflake_client.models.enums.object_type import ObjectType


def test_describe_future_grant_for_non_existing_database_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_describe_grant_for_non_existing_database_role"""
    ### Arrange ###
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=sys_admin)
    try:
            flake.register_asset_async(database, assets_queue).wait()
            ### Act ###
            grants = flake.describe_async(describable=FutureGrantDescribable(principal=DatabaseRoleDescribable(name="NON_EXISTING_DATABASE_ROLE", db_name=database.db_name))).deserialize_many(FutureGrantEntity)

            ### Assert ###

            assert grants == []
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)



def test_describe_future_grant_for_database_role_in_non_existing_database(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_describe_future_grant_for_database_role_in_non_existing_database"""
    ### Act ###
    grants = flake.describe_async(describable=FutureGrantDescribable(principal=DatabaseRoleDescribable(name="NON_EXISTING_DATABASE_ROLE", db_name="I_DONT_EXIST_EITHER_DATABASE"))).deserialize_many(FutureGrantEntity)

    ### Assert ###

    assert grants == []



def test_database_role_future_database_object_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_database_role_future_database_object_grant"""
    ### Arrange ###
    user_admin = RoleAsset("USERADMIN")
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=sys_admin)
    db_role = DatabaseRoleAsset("IGT_DB_ROLE", database.db_name, f"pyflake_client_test_{uuid.uuid4()}", user_admin)
    grant = GrantAction(db_role, DatabaseObjectFutureGrant(database_name=database.db_name, grant_target=ObjectType.TABLE), [Privilege.SELECT, Privilege.REFERENCES])

    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.register_asset_async(db_role, assets_queue).wait()
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(describable=FutureGrantDescribable(principal=DatabaseRoleDescribable(name=db_role.name, db_name=database.db_name))).deserialize_many(FutureGrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        select = next((r for r in grants if r.privilege == Privilege.SELECT), None)
        assert select is not None
        assert select.grant_on == ObjectType.TABLE
        assert select.grant_identifier == f"{database.db_name}.<{ObjectType.TABLE}>"
        assert select.grantee_identifier == db_role.name
        assert select.grantee_type == "DATABASE_ROLE"

        references = next((r for r in grants if r.privilege == Privilege.REFERENCES), None)
        assert references is not None
        assert references.grant_on == ObjectType.TABLE
        assert select.grant_identifier == f"{database.db_name}.<{ObjectType.TABLE}>"
        assert references.grantee_identifier == db_role.name
        assert references.grantee_type == "DATABASE_ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_database_role_schema_object_future_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_database_role_schema_object_future_grant"""
    ### Arrange ###
    user_admin = RoleAsset("USERADMIN")
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", sys_admin)
    schema = SchemaAsset(database.db_name, "IGT_SCHEMA", f"pyflake_client_test_{uuid.uuid4()}", sys_admin)
    db_role = DatabaseRoleAsset("IGT_DB_ROLE", database.db_name, f"pyflake_client_test_{uuid.uuid4()}", user_admin)
    grant = GrantAction(db_role, SchemaObjectFutureGrant(database_name=database.db_name, schema_name=schema.schema_name, grant_target=ObjectType.TABLE), [Privilege.SELECT, Privilege.REFERENCES])

    try:
        flake.register_asset_async(database, assets_queue).wait()
        w1 = flake.register_asset_async(schema, assets_queue)
        w2 = flake.register_asset_async(db_role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(describable=FutureGrantDescribable(principal=DatabaseRoleDescribable(name=db_role.name, db_name=database.db_name))).deserialize_many(FutureGrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        select = next((r for r in grants if r.privilege == Privilege.SELECT), None)
        assert select is not None
        assert select.grant_on == ObjectType.TABLE
        assert select.grant_identifier == f"{database.db_name}.{schema.schema_name}.<{ObjectType.TABLE}>"
        assert select.grantee_identifier == db_role.name
        assert select.grantee_type == "DATABASE_ROLE"

        references = next((r for r in grants if r.privilege == Privilege.REFERENCES), None)
        assert references is not None
        assert references.grant_on == ObjectType.TABLE
        assert select.grant_identifier == f"{database.db_name}.{schema.schema_name}.<{ObjectType.TABLE}>"
        assert references.grantee_identifier == db_role.name
        assert references.grantee_type == "DATABASE_ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)