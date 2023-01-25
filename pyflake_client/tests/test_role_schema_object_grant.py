"""test_role_schema_object_grant"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
import queue

from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.enums.object_type import ObjectType

from pyflake_client.models.assets.grant import Grant
from pyflake_client.models.assets.table import Column, Table
from pyflake_client.models.assets.grants.role_schema_future_grant import RoleSchemaFutureGrant as RoleSchemaFutureGrantAsset
from pyflake_client.pyflake_client import PyflakeClient
from pyflake_client.tests.utilities import _spawn_without_rwc_privileges, compare

from pyflake_client.models.describables.grants.role_schema_object_grant import RoleSchemaObjectGrant
from pyflake_client.models.entities.grants.role_schema_object_grant import RoleSchemaObjectGrants


def test_get_granted_privileges_r(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_get_granted_privileges_r"""
    ### Arrange ###
    d, s, r, rw, rwc = _spawn_without_rwc_privileges(flake, assets_queue)
    table = ObjectType.TABLE
    r_privilege = Grant(RoleSchemaFutureGrantAsset(r.name, d.db_name, s.schema_name, table), ["SELECT", "REFERENCES"])
    rw_privilege = Grant(RoleSchemaFutureGrantAsset(rw.name, d.db_name, s.schema_name, table), ["INSERT", "UPDATE"])
    rwc_privilege = Grant(RoleSchemaFutureGrantAsset(rwc.name, d.db_name, s.schema_name, table), ["OWNERSHIP"])
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s, "TEST", table_columns)

    try:
        flake.register_asset(r_privilege, assets_queue)
        flake.register_asset(rw_privilege, assets_queue)
        flake.register_asset(rwc_privilege, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = RoleSchemaObjectGrant(r.name, d.db_name, s.schema_name, ObjectType.TABLE, t.table_name)
        rp: RoleSchemaObjectGrants = flake.describe(showable, RoleSchemaObjectGrants)

        ### Assert ###
        assert rp.role_name == r.name
        assert rp.db_name == d.db_name
        assert rp.schema_name == s.schema_name
        assert rp.object_name == t.table_name
        assert rp.object_type == ObjectType.TABLE
        assert len(rp.inherited_privileges) == 0
        assert compare(rp.granted_privileges, r_privilege.privileges)
        assert compare(rp.all_privileges, r_privilege.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_granted_privileges_rw(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_get_granted_privileges_rw"""
    ### Arrange ###
    d, s, r, rw, rwc = _spawn_without_rwc_privileges(flake, assets_queue)
    table = ObjectType.TABLE
    r_privilege = Grant(RoleSchemaFutureGrantAsset(r.name, d.db_name, s.schema_name, table), ["SELECT", "REFERENCES"])
    rw_privilege = Grant(RoleSchemaFutureGrantAsset(rw.name, d.db_name, s.schema_name, table), ["INSERT", "UPDATE"])
    rwc_privilege = Grant(RoleSchemaFutureGrantAsset(rwc.name, d.db_name, s.schema_name, table), ["OWNERSHIP"])
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s, "TEST", table_columns)

    try:
        flake.register_asset(r_privilege, assets_queue)
        flake.register_asset(rw_privilege, assets_queue)
        flake.register_asset(rwc_privilege, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = RoleSchemaObjectGrant(rw.name, d.db_name, s.schema_name, ObjectType.TABLE, t.table_name)
        rp: RoleSchemaObjectGrants = flake.describe(showable, RoleSchemaObjectGrants)

        ### Assert ###
        assert rp.role_name == rw.name
        assert rp.db_name == d.db_name
        assert rp.schema_name == s.schema_name
        assert rp.object_name == t.table_name
        assert rp.object_type == ObjectType.TABLE
        assert len(rp.inherited_privileges) == 1
        assert compare(rp.granted_privileges, rw_privilege.privileges)
        assert compare(rp.all_privileges, r_privilege.privileges + rw_privilege.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_granted_privileges_rwc(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_get_granted_privileges_rwc"""
    ### Arrange ###
    d, s, r, rw, rwc = _spawn_without_rwc_privileges(flake, assets_queue)
    table = ObjectType.TABLE
    r_privilege = Grant(RoleSchemaFutureGrantAsset(r.name, d.db_name, s.schema_name, table), ["SELECT", "REFERENCES"])
    rw_privilege = Grant(RoleSchemaFutureGrantAsset(rw.name, d.db_name, s.schema_name, table), ["INSERT", "UPDATE"])
    rwc_privilege = Grant(RoleSchemaFutureGrantAsset(rwc.name, d.db_name, s.schema_name, table), ["OWNERSHIP"])
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s, "TEST", table_columns)

    try:
        flake.register_asset(r_privilege, assets_queue)
        flake.register_asset(rw_privilege, assets_queue)
        flake.register_asset(rwc_privilege, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = RoleSchemaObjectGrant(rwc.name, d.db_name, s.schema_name, ObjectType.TABLE, t.table_name)
        rp: RoleSchemaObjectGrants = flake.describe(showable, RoleSchemaObjectGrants)

        ### Assert ###
        assert rp.role_name == rwc.name
        assert rp.db_name == d.db_name
        assert rp.schema_name == s.schema_name
        assert rp.object_name == t.table_name
        assert rp.object_type == ObjectType.TABLE
        assert len(rp.inherited_privileges) == 2
        assert compare(rp.granted_privileges, rwc_privilege.privileges)
        assert compare(rp.all_privileges, r_privilege.privileges + rw_privilege.privileges + rwc_privilege.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_granted_privileges_accountadmin(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """
    test_get_granted_privileges_accountadmin:
    from _spawn_without_rwc_privileges() we get the following role hierarchy: r >> rw >> rwc >> db_sys_admin >> SYSADMIN >> ACCOUNTADMIN
    """
    ### Arrange ###
    d, s, r, rw, rwc = _spawn_without_rwc_privileges(flake, assets_queue)
    table = ObjectType.TABLE
    r_privilege = Grant(RoleSchemaFutureGrantAsset(r.name, d.db_name, s.schema_name, table), ["SELECT", "REFERENCES"])
    rw_privilege = Grant(RoleSchemaFutureGrantAsset(rw.name, d.db_name, s.schema_name, table), ["INSERT", "UPDATE"])
    rwc_privilege = Grant(RoleSchemaFutureGrantAsset(rwc.name, d.db_name, s.schema_name, table), ["OWNERSHIP"])
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s, "TEST", table_columns)

    try:
        flake.register_asset(r_privilege, assets_queue)
        flake.register_asset(rw_privilege, assets_queue)
        flake.register_asset(rwc_privilege, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = RoleSchemaObjectGrant("ACCOUNTADMIN", d.db_name, s.schema_name, ObjectType.TABLE, t.table_name)
        rp: RoleSchemaObjectGrants = flake.describe(showable, RoleSchemaObjectGrants)

        ### Assert ###
        assert rp.role_name == "ACCOUNTADMIN"
        assert rp.db_name == d.db_name
        assert rp.schema_name == s.schema_name
        assert rp.object_name == t.table_name
        assert rp.object_type == ObjectType.TABLE
        assert len(rp.granted_privileges) == 0
        assert len(rp.inherited_privileges) == 3
        assert compare(rp.all_privileges, r_privilege.privileges + rw_privilege.privileges + rwc_privilege.privileges)
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_granted_privileges_useradmin(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """
    test_get_granted_privileges_useradmin:
    from _spawn() we get the following role hierarchy: r >> rw >> rwc >> db_sys_admin >> SYSADMIN >> ACCOUNTADMIN
    i.e. USERADMIN should not have any privileges on any schema objects
    """
    ### Arrange ###
    d, s, r, rw, rwc = _spawn_without_rwc_privileges(flake, assets_queue)
    table = ObjectType.TABLE
    r_privilege = Grant(RoleSchemaFutureGrantAsset(r.name, d.db_name, s.schema_name, table), ["SELECT", "REFERENCES"])
    rw_privilege = Grant(RoleSchemaFutureGrantAsset(rw.name, d.db_name, s.schema_name, table), ["INSERT", "UPDATE"])
    rwc_privilege = Grant(RoleSchemaFutureGrantAsset(rwc.name, d.db_name, s.schema_name, table), ["OWNERSHIP"])
    table_columns = [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_VARCHAR", ColumnType.VARCHAR, primary_key=True)
    ]
    t = Table(s, "TEST", table_columns)

    try:
        flake.register_asset(r_privilege, assets_queue)
        flake.register_asset(rw_privilege, assets_queue)
        flake.register_asset(rwc_privilege, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        showable = RoleSchemaObjectGrant("USERADMIN", d.db_name, s.schema_name, ObjectType.TABLE, t.table_name)
        rp: RoleSchemaObjectGrants = flake.describe(showable, RoleSchemaObjectGrants)

        ### Assert ###
        assert rp.role_name == "USERADMIN"
        assert rp.db_name == d.db_name
        assert rp.schema_name == s.schema_name
        assert rp.object_name == t.table_name
        assert rp.object_type == ObjectType.TABLE
        assert len(rp.granted_privileges) == 0
        assert len(rp.inherited_privileges) == 0
        assert len(rp.all_privileges) == 0
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
