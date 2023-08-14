# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
import os
import queue
import secrets
import uuid
from typing import Generator, Tuple

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.grant_action import GrantAction
from pyflake_client.models.assets.grants.schema_object_future_grant import (
    SchemaObjectFutureGrant,
)
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.role_inheritance import RoleInheritance
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.enums.privilege import Privilege


# https://docs.pytest.org/en/6.2.x/fixture.html#fixture-scopes
@pytest.fixture(scope="session")
def flake() -> Generator[PyflakeClient, None, None]:
    """flake"""

    conn: SnowflakeConnection = snowflake.connector.connect(
        host=os.getenv("SNOWFLAKE_HOST"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_UID"),
        password=os.getenv("SNOWFLAKE_PWD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WH"),
        autocommit=True,
    )

    cli = PyflakeClient(conn)

    yield cli

    conn.close()


@pytest.fixture(scope="function")
def assets_queue() -> queue.LifoQueue:
    """assets_queue"""
    return queue.LifoQueue()


@pytest.fixture(scope="session")
def comment() -> str:
    return f"pyflake_client_test_{uuid.uuid4()}"


@pytest.fixture(scope="session")
def proc_db(
    flake: PyflakeClient,
) -> Generator[Tuple[DatabaseAsset, SchemaAsset, DatabaseRole, DatabaseRole, DatabaseRole], None, None]:
    """bootstrap utility function"""
    asset_queue = queue.LifoQueue()
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    db_name = f"PYFLAKE_CLIENT_DB_{secrets.token_hex(5)}".upper()
    schema_name = "CONFIG"
    user_admin_role = RoleAsset("USERADMIN")
    sys_admin_role = RoleAsset("SYSADMIN")
    db: DatabaseAsset = DatabaseAsset(db_name, snowflake_comment, sys_admin_role)
    db_sys_admin = DatabaseRole(f"{db_name}_SYS_ADMIN", db.db_name, snowflake_comment, user_admin_role)
    db_usr_admin = DatabaseRole(f"{db_name}_USER_ADMIN", db.db_name, snowflake_comment, user_admin_role)
    rel1 = RoleInheritance(db_sys_admin, sys_admin_role)
    r = DatabaseRole(f"{db.db_name}_{schema_name}_ACCESS_ROLE_R", db.db_name, snowflake_comment, user_admin_role)
    rw = DatabaseRole(f"{db.db_name}_{schema_name}_ACCESS_ROLE_RW", db.db_name, snowflake_comment, user_admin_role)
    rwc = DatabaseRole(f"{db.db_name}_{schema_name}_ACCESS_ROLE_RWC", db.db_name, snowflake_comment, user_admin_role)
    rel2 = RoleInheritance(r, rw)
    rel3 = RoleInheritance(rw, rwc)
    rel4 = RoleInheritance(rwc, db_sys_admin)
    rel5 = RoleInheritance(db_sys_admin, sys_admin_role)
    s = SchemaAsset(db_name=db.db_name, schema_name=schema_name, comment=snowflake_comment, owner=db_sys_admin)

    table_privilege_r = GrantAction(
        r,
        SchemaObjectFutureGrant(database_name=db.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        [Privilege.SELECT, Privilege.REFERENCES],
    )
    table_privilege_rw = GrantAction(
        rw,
        SchemaObjectFutureGrant(database_name=db.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        [Privilege.INSERT, Privilege.UPDATE, Privilege.DELETE, Privilege.TRUNCATE],
    )
    table_privilege_rwc = GrantAction(
        rwc,
        SchemaObjectFutureGrant(database_name=db.db_name, schema_name=s.schema_name, grant_target=ObjectType.TABLE),
        [Privilege.OWNERSHIP],
    )
    procedure_privilege_r = GrantAction(
        r,
        SchemaObjectFutureGrant(
            database_name=db.db_name, schema_name=s.schema_name, grant_target=ObjectType.PROCEDURE
        ),
        [Privilege.USAGE],
    )
    procedure_privilege_rwc = GrantAction(
        rwc,
        SchemaObjectFutureGrant(
            database_name=db.db_name, schema_name=s.schema_name, grant_target=ObjectType.PROCEDURE
        ),
        [Privilege.OWNERSHIP],
    )
    try:
        flake.register_asset_async(db, asset_queue).wait()

        dbr1 = flake.create_asset_async(db_sys_admin)
        dbr2 = flake.create_asset_async(db_usr_admin)
        dbr3 = flake.create_asset_async(r)
        dbr4 = flake.create_asset_async(rw)
        dbr5 = flake.create_asset_async(rwc)
        s1 = flake.create_asset_async(s)
        flake.wait_all([dbr1, dbr2, dbr3, dbr4, dbr5, s1])

        ih1 = flake.create_asset_async(rel1)
        ih2 = flake.create_asset_async(rel2)
        ih3 = flake.create_asset_async(rel3)
        ih4 = flake.create_asset_async(rel4)
        ih5 = flake.create_asset_async(rel5)
        flake.wait_all([ih1, ih2, ih3, ih4, ih5])

        p1 = flake.create_asset_async(table_privilege_rwc)
        p2 = flake.create_asset_async(procedure_privilege_rwc)
        p3 = flake.create_asset_async(table_privilege_r)
        p4 = flake.create_asset_async(table_privilege_rw)
        p5 = flake.create_asset_async(procedure_privilege_r)
        # flake.register_asset(procedure_privilege_rw, assets_queue)
        flake.wait_all([p1, p2, p3, p4, p5])

        yield db, s, r, rw, rwc
    finally:
        flake.delete_assets(asset_queue)
