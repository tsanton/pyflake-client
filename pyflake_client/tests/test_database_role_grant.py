"""test_database"""
import queue
import uuid

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.grants.schema_grant import SchemaGrant
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.assets.grants.database_grant import DatabaseGrant
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset

from pyflake_client.models.assets.grant_action import GrantAction

from pyflake_client.models.describables.database_role import DatabaseRole as DatabaseRoleDescribable
from pyflake_client.models.describables.grant import Grant as GrantDescribable

from pyflake_client.models.entities.grant import Grant as GrantEntity

from pyflake_client.models.enums.privilege import Privilege

#TODO: test_describe_grant_for_non_existing_database_role

def test_describe_grant_for_non_existing_database_role(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_describe_grant_for_non_existing_database_role"""
    ### Arrange ###
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=sys_admin)
    try:
            flake.register_asset(database, assets_queue)
            ### Act ###
            grants = flake.describe_many(describable=GrantDescribable(principal=DatabaseRoleDescribable(name="NON_EXISTING_DATABASE_ROLE", db_name=database.db_name)), entity=GrantEntity)

            ### Assert ###

            assert grants is None
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_database_role_database_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_database_role_database_grant"""
    ### Arrange ###
    user_admin = RoleAsset("USERADMIN")
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=sys_admin)
    db_role = DatabaseRoleAsset("IGT_DB_ROLE", database.db_name, f"pyflake_client_test_{uuid.uuid4()}", user_admin)
    grant = GrantAction(db_role, DatabaseGrant(database_name=database.db_name), [Privilege.CREATE_DATABASE_ROLE, Privilege.CREATE_SCHEMA])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(db_role, assets_queue)
        flake.register_asset(grant, assets_queue)

        ### Act ###
        grants = flake.describe_many(describable=GrantDescribable(principal=DatabaseRoleDescribable(name=db_role.name, db_name=database.db_name)), entity=GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 3

        usg = next((r for r in grants if r.privilege == Privilege.USAGE and r.granted_on == "DATABASE"), None)
        assert usg is not None
        assert usg.granted_by == "" #Implisit usage grant on databases for database roles
        assert usg.grantee_identifier == db_role.name
        assert usg.grantee_type == "DATABASE_ROLE"
        assert usg.granted_identifier == database.db_name

        cdr = next((r for r in grants if r.privilege == Privilege.CREATE_DATABASE_ROLE and r.granted_on == "DATABASE"), None)
        assert cdr is not None
        assert cdr.granted_on == "DATABASE"
        assert cdr.granted_by == "SYSADMIN"
        assert cdr.grantee_identifier == db_role.name
        assert cdr.grantee_type == "DATABASE_ROLE"
        assert cdr.granted_identifier == database.db_name

        cs = next((r for r in grants if r.privilege == Privilege.CREATE_SCHEMA and r.granted_on == "DATABASE"), None)
        assert cs is not None
        assert cs.granted_on == "DATABASE"
        assert cs.granted_by == "SYSADMIN"
        assert cs.grantee_identifier == db_role.name
        assert cs.grantee_type == "DATABASE_ROLE"
        assert cs.granted_identifier == database.db_name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_database_role_schema_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_database_role_schema_grant"""
    ### Arrange ###
    user_admin = RoleAsset("USERADMIN")
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=sys_admin)
    schema = SchemaAsset(database, "IGT_SCHEMA", f"pyflake_client_test_{uuid.uuid4()}", sys_admin)
    db_role = DatabaseRoleAsset("IGT_DB_ROLE", database.db_name, f"pyflake_client_test_{uuid.uuid4()}", user_admin)
    grant = GrantAction(db_role, SchemaGrant(database_name=database.db_name, schema_name=schema.schema_name), [Privilege.USAGE, Privilege.CREATE_TABLE])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(db_role, assets_queue)
        flake.register_asset(grant, assets_queue)

        ### Act ###
        grants = flake.describe_many(describable=GrantDescribable(principal=DatabaseRoleDescribable(name=db_role.name, db_name=database.db_name)), entity=GrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 3

        usg = next((r for r in grants if r.privilege == Privilege.USAGE and r.granted_on == "DATABASE"), None)
        assert usg is not None
        assert usg.granted_by == "" #Implisit usage grant on databases for database roles
        assert usg.grantee_identifier == db_role.name
        assert usg.grantee_type == "DATABASE_ROLE"
        assert usg.granted_identifier == database.db_name

        susg = next((r for r in grants if r.privilege == Privilege.USAGE and r.granted_on == "SCHEMA"), None)
        assert susg is not None
        assert susg.granted_by == "SYSADMIN"
        assert susg.grantee_identifier == db_role.name
        assert susg.grantee_type == "DATABASE_ROLE"
        assert susg.granted_identifier == f"{database.db_name}.{schema.schema_name}"

        ct = next((r for r in grants if r.privilege == Privilege.CREATE_TABLE and r.granted_on == "SCHEMA"), None)
        assert ct is not None
        assert ct.granted_by == "SYSADMIN"
        assert ct.grantee_identifier == db_role.name
        assert ct.grantee_type == "DATABASE_ROLE"
        assert ct.granted_identifier == f"{database.db_name}.{schema.schema_name}"
    
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)