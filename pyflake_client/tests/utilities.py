# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

import collections
import queue
import uuid
from typing import Callable, List, Optional, Tuple, TypeVar

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.grant_action import GrantAction
from pyflake_client.models.assets.grants.schema_object_future_grant import (
    SchemaObjectFutureGrant,
)
from pyflake_client.models.assets.role import Role
from pyflake_client.models.assets.role_inheritance import RoleInheritance
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.enums.privilege import Privilege

T = TypeVar("T")


def compare(x, y) -> bool:
    """compare:
    - compare([1,2,3], [1,2,3,3]) -> false
    - compare([1,2,3], [1,2,4]) -> false
    - compare([1,2,3], [1,2,3]) -> true
    - compare([1,2,3], [3,2,1]) -> true
    """
    return collections.Counter(x) == collections.Counter(y)


def find(collection: List[T], predicate: Callable[[T], bool]) -> Optional[T]:
    for item in collection:
        if predicate(item):
            return item
    return None


def _spawn_with_rwc_privileges(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
) -> Tuple[Database, Schema, Role, Role, Role]:
    """bootstrap utility function"""
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    db_name = "IGT_DEMO"
    schema_name = "MGMT"
    user_admin_role = Role("USERADMIN")
    sys_admin_role = Role("SYSADMIN")
    db_sys_admin = Role(f"{db_name}_SYS_ADMIN", snowflake_comment, user_admin_role)
    db_usr_admin = Role(f"{db_name}_USER_ADMIN", snowflake_comment, user_admin_role)
    r1 = RoleInheritance(db_sys_admin, sys_admin_role)
    d: Database = Database("IGT_DEMO", snowflake_comment, Role(db_sys_admin.name))
    r = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_R", snowflake_comment, user_admin_role)
    rw = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RW", snowflake_comment, user_admin_role)
    rwc = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RWC", snowflake_comment, user_admin_role)
    r2 = RoleInheritance(r, rw)
    r3 = RoleInheritance(rw, rwc)
    r4 = RoleInheritance(rwc, sys_admin_role)
    s: Schema = Schema(db_name=d.db_name, schema_name=schema_name, comment=snowflake_comment, owner=Role(rwc.name))

    r_table = [Privilege.SELECT, Privilege.REFERENCES]
    rw_table = [Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE, Privilege.TRUNCATE]
    r_procedure = [Privilege.USAGE]

    # rw_procedure = []
    rwcp = [Privilege.OWNERSHIP]
    table_privilege_r = GrantAction(
        r,
        SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        r_table,
    )
    table_privilege_rw = GrantAction(
        rw,
        SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        rw_table,
    )
    table_privilege_rwc = GrantAction(
        rwc,
        SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        rwcp,
    )

    procedure_privilege_r = GrantAction(
        r,
        SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.PROCEDURE),
        r_procedure,
    )
    # procedure_privilege_rw = GrantAction(rw, SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.PROCEDURE), rw_procedure)
    procedure_privilege_rwc = GrantAction(
        rwc,
        SchemaObjectFutureGrant(database_name=d.db_name, schema_name=s.schema_name, grant_target=ObjectType.PROCEDURE),
        rwcp,
    )
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


# def _spawn_database_and_schema(flake: PyflakeClient, assets_queue: queue.LifoQueue) -> Tuple[Database, Schema, Schema]:
#     """_spawn_database_and_schema"""
#     snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
#     db_name = "IGT_DEMO"
#     user_admin_role = Role("SYSADMIN")
#     sys_admin_role = Role("USERADMIN")
#     db_sys_admin = Role(f"{db_name}_SYS_ADMIN", snowflake_comment, user_admin_role)
#     database: Database = Database(db_name=db_name, comment=snowflake_comment, owner=sys_admin_role)
#     schema1: Schema = Schema(database=database, schema_name="SCHEMA1", comment=snowflake_comment, owner=sys_admin_role)
#     schema2: Schema = Schema(database=database, schema_name="SCHEMA2", comment=snowflake_comment, owner=sys_admin_role)

#     try:
#         flake.register_asset(db_sys_admin, assets_queue)
#         flake.register_asset(database, assets_queue)
#         flake.register_asset(schema1, assets_queue)
#         flake.register_asset(schema2, assets_queue)
#     except Exception as err:
#         flake.delete_assets(assets_queue)
#         raise err

#     return database, schema1, schema2


# def _spawn_without_rwc_privileges(flake: PyflakeClient, assets_queue: queue.LifoQueue) -> Tuple[Database, Schema, Role, Role, Role]:
#     snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
#     db_name = "IGT_DEMO"
#     schema_name = "SCHEMA1"
#     user_admin_role = Role("SYSADMIN")
#     sys_admin_role = Role("USERADMIN")
#     db_sys_admin = Role(f"{db_name}_SYS_ADMIN", snowflake_comment, user_admin_role)
#     db_usr_admin = Role(f"{db_name}_USER_ADMIN", snowflake_comment, user_admin_role)
#     r1 = RoleInheritance(db_sys_admin, sys_admin_role)
#     d: Database = Database("IGT_DEMO", snowflake_comment, owner=Role(db_sys_admin.name))
#     r = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_R", snowflake_comment, user_admin_role)
#     rw = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RW", snowflake_comment, user_admin_role)
#     rwc = Role(f"{d.db_name}_{schema_name}_ACCESS_ROLE_RWC", snowflake_comment, user_admin_role)
#     r2 = RoleInheritance(r, rw)
#     r3 = RoleInheritance(rw, rwc)
#     r4 = RoleInheritance(rwc, sys_admin_role)
#     s: Schema = Schema(database=d, schema_name=schema_name, comment=snowflake_comment, owner=Role(rwc.name))

#     try:
#         flake.register_asset(db_sys_admin, assets_queue)
#         flake.register_asset(db_usr_admin, assets_queue)
#         flake.register_asset(r1, assets_queue)
#         flake.register_asset(d, assets_queue)
#         flake.register_asset(r, assets_queue)
#         flake.register_asset(rw, assets_queue)
#         flake.register_asset(rwc, assets_queue)
#         flake.register_asset(r2, assets_queue)
#         flake.register_asset(r3, assets_queue)
#         flake.register_asset(r4, assets_queue)
#         flake.register_asset(s, assets_queue)
#     except Exception as err:
#         flake.delete_assets(assets_queue)
#         raise err

#     return d, s, r, rw, rwc
