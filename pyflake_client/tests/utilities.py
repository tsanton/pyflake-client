"""utilities"""
# pylint: disable=invalid-name
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import collections
import queue
from typing import Tuple
import uuid
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.role_relationship import RoleRelationship
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.grant import Grant
from pyflake_client.models.assets.grants.role_schema_future_grant import RoleSchemaFutureGrant
from pyflake_client.models.enums.object_type import ObjectType

from pyflake_client.pyflake_client import PyflakeClient


def compare(x, y) -> bool:
    """compare:
       - compare([1,2,3], [1,2,3,3]) -> false
       - compare([1,2,3], [1,2,4]) -> false
       - compare([1,2,3], [1,2,3]) -> true
       - compare([1,2,3], [3,2,1]) -> true
    """
    return collections.Counter(x) == collections.Counter(y)


def _spawn_with_rwc_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue) -> Tuple[Database, Schema, Role, Role, Role]:
    """bootstrap utility function"""
    db_name = "IGT_DEMO"
    schema_name = "MGMT"
    db_sys_admin = Role(f"{db_name}_SYS_ADMIN", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    db_usr_admin = Role(f"{db_name}_USER_ADMIN", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    r1 = RoleRelationship(db_sys_admin.name, "SYSADMIN")
    d: Database = Database("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=db_sys_admin.name)
    r = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_R", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    rw = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RW", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    rwc = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RWC", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    r2 = RoleRelationship(r.name, rw.name)
    r3 = RoleRelationship(rw.name, rwc.name)
    r4 = RoleRelationship(rwc.name, "SYSADMIN")
    s: Schema = Schema(database=d, schema_name=schema_name, comment=f"pyflake_client_TEST_{uuid.uuid4()}", owner=rwc.name)

    r_table = ["SELECT", "REFERENCES"]
    r_procedure = ["USAGE"]
    rw_table = ["INSERT", "UPDATE", "DELETE", "TRUNCATE"]
    # rw_procedure = []
    rwcp = ["OWNERSHIP"]
    table_privilege_r = Grant(RoleSchemaFutureGrant(r.name, d.db_name, s.schema_name, ObjectType.TABLE), r_table)
    table_privilege_rw = Grant(RoleSchemaFutureGrant(rw.name, d.db_name, s.schema_name, ObjectType.TABLE), rw_table)
    table_privilege_rwc = Grant(RoleSchemaFutureGrant(rwc.name, d.db_name, s.schema_name, ObjectType.TABLE), rwcp)

    procedure_privilege_r = Grant(RoleSchemaFutureGrant(r.name, d.db_name, s.schema_name, ObjectType.PROCEDURE), r_procedure)
    # procedure_privilege_rw = Grant(RoleSchemaFutureGrant(rw.name, d.db_name, s.schema_name, ObjectType.PROCEDURE), rw_procedure)
    procedure_privilege_rwc = Grant(RoleSchemaFutureGrant(rwc.name, d.db_name, s.schema_name, ObjectType.PROCEDURE), rwcp)
    try:
        flake.register_asset(db_sys_admin, assets_queue)
        flake.register_asset(db_usr_admin, assets_queue)
        flake.register_asset(r1, assets_queue)
        flake.register_asset(d, assets_queue)
        flake.register_asset(r, assets_queue)
        flake.register_asset(rw, assets_queue)
        flake.register_asset(rwc, assets_queue)
        flake.register_asset(r2, assets_queue)
        flake.register_asset(r3, assets_queue)
        flake.register_asset(r4, assets_queue)
        flake.register_asset(s, assets_queue)
        flake.register_asset(table_privilege_r, assets_queue)
        flake.register_asset(table_privilege_rw, assets_queue)
        flake.register_asset(table_privilege_rwc, assets_queue)
        flake.register_asset(procedure_privilege_r, assets_queue)
        # flake.register_asset(procedure_privilege_rw, assets_queue)
        flake.register_asset(procedure_privilege_rwc, assets_queue)
    except Exception as err:
        flake.delete_assets(assets_queue)
        raise err

    return d, s, r, rw, rwc


def _spawn_database_and_schema(flake: PyflakeClient, assets_queue: queue.LifoQueue) -> Tuple[Database, Schema, Schema]:
    """_spawn_database_and_schema"""
    database: Database = Database("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}")
    schema1: Schema = Schema(database=database, schema_name="SCHEMA1", comment=f"pyflake_client_TEST_{uuid.uuid4()}")
    schema2: Schema = Schema(database=database, schema_name="SCHEMA2", comment=f"pyflake_client_TEST_{uuid.uuid4()}")

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema1, assets_queue)
        flake.register_asset(schema2, assets_queue)
    except Exception as err:
        flake.delete_assets(assets_queue)
        raise err

    return database, schema1, schema2


def _spawn_without_rwc_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue) -> Tuple[Database, Schema, Role, Role, Role]:
    db_name = "IGT_DEMO"
    schema_name = "SCHEMA1"
    db_sys_admin = Role(f"{db_name}_SYS_ADMIN", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    db_usr_admin = Role(f"{db_name}_USER_ADMIN", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    r1 = RoleRelationship(db_sys_admin.name, "SYSADMIN")
    d: Database = Database("IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=db_sys_admin.name)
    r = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_R", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    rw = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RW", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    rwc = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RWC", "USERADMIN", f"pyflake_client_TEST_{uuid.uuid4()}")
    r2 = RoleRelationship(r.name, rw.name)
    r3 = RoleRelationship(rw.name, rwc.name)
    r4 = RoleRelationship(rwc.name, "SYSADMIN")
    s: Schema = Schema(database=d, schema_name=schema_name, comment=f"pyflake_client_TEST_{uuid.uuid4()}", owner=rwc.name)

    try:
        flake.register_asset(db_sys_admin, assets_queue)
        flake.register_asset(db_usr_admin, assets_queue)
        flake.register_asset(r1, assets_queue)
        flake.register_asset(d, assets_queue)
        flake.register_asset(r, assets_queue)
        flake.register_asset(rw, assets_queue)
        flake.register_asset(rwc, assets_queue)
        flake.register_asset(r2, assets_queue)
        flake.register_asset(r3, assets_queue)
        flake.register_asset(r4, assets_queue)
        flake.register_asset(s, assets_queue)
    except Exception as err:
        flake.delete_assets(assets_queue)
        raise err

    return d, s, r, rw, rwc
