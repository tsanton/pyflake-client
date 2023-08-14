# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import queue
import secrets
from datetime import date
from typing import Tuple

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.procedure import Procedure as ProcedureAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.describables.procedure import (
    Procedure as ProcedureDescribable,
)
from pyflake_client.models.entities.procedure import Procedure as ProcedureEntity
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.tests.utilities import compare


def test_create_procedure_zero_args(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_create_procedure_zero_args"""
    ### Arrange ###
    db, s, _, _, _ = proc_db
    proc_name = f"TEST_PROC_{secrets.token_hex(5)}".upper()
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.{proc_name}()
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT ('Hello you');
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(db.db_name, s.schema_name, proc_name, [], sql)

    try:
        flake.register_asset_async(proc, assets_queue).wait()
        sf_proc = flake.describe_async(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name),
        ).deserialize_one(ProcedureEntity)
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_procedure_one_arg(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_create_procedure_one_arg"""
    ### Arrange ###
    db, s, _, _, _ = proc_db
    proc_name = f"TEST_PROC_{secrets.token_hex(5)}".upper()
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.{proc_name}(NAME VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT ('Hello ', NAME);
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(db.db_name, s.schema_name, proc_name, [ColumnType.VARCHAR], sql)

    try:
        flake.register_asset_async(proc, assets_queue)
        sf_proc = flake.describe_async(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name),
        ).deserialize_one(ProcedureEntity)
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert sf_proc.created_on.date() == date.today()
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_procedure_multiple_args(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_create_procedure_multiple_args"""
    ### Arrange ###
    db, s, _, _, _ = proc_db
    proc_name = f"TEST_PROC_{secrets.token_hex(5)}".upper()
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.{proc_name}(MESSAGE VARCHAR(16777216), NAME VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT (MESSAGE, ' ', NAME);
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(
        db.db_name,
        s.schema_name,
        proc_name,
        [ColumnType.VARCHAR, ColumnType.VARCHAR],
        sql,
    )

    try:
        flake.register_asset_async(proc, assets_queue)
        sf_proc = flake.describe_async(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name)
        ).deserialize_one(ProcedureEntity)
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert sf_proc.created_on.date() == date.today()
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
