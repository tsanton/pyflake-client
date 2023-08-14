# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import queue
import secrets
from typing import Tuple

from snowflake.snowpark.row import Row

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.database_role import DatabaseRole
from pyflake_client.models.assets.procedure import Procedure as ProcedureAsset
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.executables.procedure import Procedure as ProcedureExec
from pyflake_client.models.executables.procedure_arg import ProcedureArg


def test_call_procedure_zero_args_string_scalar_return(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_call_procedure_zero_args_string_scalar_return"""
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
    proc_exec = ProcedureExec(db.db_name, s.schema_name, proc_name, [])

    try:
        flake.register_asset_async(proc, assets_queue).wait()

        ### Act ###
        def deserializer_func(data: Row) -> str:
            return data

        res = flake.call_async(proc_exec.get_call_statement()).fetch_one(str, deserializer_func)
        ### Assert ###
        assert res == "Hello you"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_procedure_one_arg_string_scalar_return(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_call_procedure_one_arg_string_scalar_return"""
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
    proc_exec = ProcedureExec(db.db_name, s.schema_name, proc_name, [ProcedureArg(1, ColumnType.VARCHAR, "Tullebukk")])

    try:
        flake.register_asset_async(proc, assets_queue).wait()

        def deserializer_func(data: Row) -> str:
            return data

        ### Act ###
        res = flake.call_async(proc_exec.get_call_statement()).fetch_one(str, deserializer_func)
        ### Assert ###
        assert res == "Hello Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_procedure_multiple_args_string_scalar_return(
    flake: PyflakeClient,
    proc_db: Tuple[Database, Schema, DatabaseRole, DatabaseRole, DatabaseRole],
    assets_queue: queue.LifoQueue,
):
    """test_call_procedure_multiple_args"""
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
        db.db_name, s.schema_name, proc_name, [ColumnType.VARCHAR, ColumnType.VARCHAR], sql
    )
    proc_exec = ProcedureExec(
        db.db_name,
        s.schema_name,
        proc_name,
        [
            ProcedureArg(1, ColumnType.VARCHAR, "Hello you"),
            ProcedureArg(2, ColumnType.VARCHAR, "Tullebukk"),
        ],
    )

    try:
        flake.register_asset_async(proc, assets_queue).wait()

        ### Act ###
        res = flake.call_async(proc_exec.get_call_statement()).fetch_one(str, lambda x: x)
        ### Assert ###
        assert res == "Hello you Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
