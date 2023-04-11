"""test_procedures"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import queue

from pyflake_client.models.assets.procedure import Procedure as ProcedureAsset
from pyflake_client.models.describables.procedure import (
    Procedure as ProcedureDescribable,
)
from pyflake_client.models.entities.procedure import Procedure as ProcedureEntity
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.client import PyflakeClient


from pyflake_client.tests.utilities import _spawn_with_rwc_privileges, compare


def test_create_procedure_zero_args(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_create_procedure_zero_args"""
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
    proc: ProcedureAsset = ProcedureAsset(
        db.db_name, s.schema_name, "TEST_PROC", [], sql
    )

    try:
        flake.register_asset(proc, assets_queue)
        sf_proc = flake.describe_one(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name),
            ProcedureEntity,
        )
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_procedure_one_arg(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_procedure_one_arg"""
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
    proc: ProcedureAsset = ProcedureAsset(
        db.db_name, s.schema_name, "TEST_PROC", [ColumnType.VARCHAR], sql
    )

    try:
        flake.register_asset(proc, assets_queue)
        sf_proc = flake.describe_one(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name),
            ProcedureEntity,
        )
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_procedure_multiple_args(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_create_procedure_multiple_args"""
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
    proc: ProcedureAsset = ProcedureAsset(
        db.db_name,
        s.schema_name,
        "TEST_PROC",
        [ColumnType.VARCHAR, ColumnType.VARCHAR],
        sql,
    )

    try:
        flake.register_asset(proc, assets_queue)
        sf_proc = flake.describe_one(
            ProcedureDescribable(proc.database_name, proc.schema_name, proc.name),
            ProcedureEntity,
        )
        ### Assert ###
        assert sf_proc is not None
        assert sf_proc.name == proc.name
        assert sf_proc.catalog_name == proc.database_name
        assert sf_proc.schema_name == proc.schema_name
        assert compare(sf_proc.procedure_args, proc.args)

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
