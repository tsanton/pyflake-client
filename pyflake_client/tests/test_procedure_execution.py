"""test_procedures"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import queue

from pyflake_client.models.assets.procedure import Procedure as ProcedureAsset
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.executables.procedure import Procedure as ProcedureExec
from pyflake_client.pyflake_client import PyflakeClient


from pyflake_client.tests.utilities import _spawn_with_rwc_privileges


def test_execute_procedure_zero_args(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_execute_procedure_zero_args"""
    ### Arrange ###
    db, s, _, _, _ = _spawn_with_rwc_privileges(flake, assets_queue)
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.TEST_PROC()
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT ('Hello you');
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(db.db_name, s.schema_name, "TEST_PROC", [], sql)
    proc_exec = ProcedureExec(db.db_name, s.schema_name, "TEST_PROC", [])

    try:
        flake.register_asset(proc, assets_queue)

        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Hello you"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_execute_procedure_one_arg(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_execute_procedure_one_arg"""
    ### Arrange ###
    db, s, _, _, _ = _spawn_with_rwc_privileges(flake, assets_queue)
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.TEST_PROC(NAME VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT ('Hello ', NAME);
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(db.db_name, s.schema_name, "TEST_PROC", [ColumnType.VARCHAR], sql)
    proc_exec = ProcedureExec(db.db_name, s.schema_name, "TEST_PROC", ["Tullebukk"])

    try:
        flake.register_asset(proc, assets_queue)

        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Hello Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_execute_procedure_multiple_args(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_execute_procedure_multiple_args"""
    ### Arrange ###
    db, s, _, _, _ = _spawn_with_rwc_privileges(flake, assets_queue)
    sql: str = f"""
    CREATE OR REPLACE PROCEDURE {db.db_name}.{s.schema_name}.TEST_PROC(MESSAGE VARCHAR(16777216), NAME VARCHAR(16777216))
        RETURNS VARCHAR(16777216)
        LANGUAGE SQL
        EXECUTE AS CALLER
    AS $$
        BEGIN
            RETURN CONCAT (MESSAGE, ' ', NAME);
        END
    $$;
    """
    proc: ProcedureAsset = ProcedureAsset(db.db_name, s.schema_name, "TEST_PROC", [ColumnType.VARCHAR, ColumnType.VARCHAR], sql)
    proc_exec = ProcedureExec(db.db_name, s.schema_name, "TEST_PROC", ["Hello you", "Tullebukk"])

    try:
        flake.register_asset(proc, assets_queue)

        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Hello you Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
