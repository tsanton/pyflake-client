"""test_procedures"""
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import queue

from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.executables.anonymous_procedure import AnonymousProcedure as AnonymousProcedureExec
from pyflake_client.models.executables.procedure_arg import ProcedureArg
from pyflake_client.client import PyflakeClient


# from pyflake_client.tests.utilities import _spawn_with_rwc_privileges


def test_call_anonymous_procedure_zero_args(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_call_procedure_zero_args"""
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARCHAR not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return 'Hello you!'
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    try:
        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Hello you!"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_anonymous_procedure_zero_args_true(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_call_anonymous_procedure_zero_args_true"""
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns BOOLEAN not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return True
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    try:
        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res is True

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_anonymous_procedure_zero_args_false(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_call_anonymous_procedure_zero_args_false"""
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns BOOLEAN not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return False
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    try:
        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res is False

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_anonymous_procedure_one_arg(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_call_procedure_one_arg"""
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure(name VARCHAR)
    returns VARCHAR not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session, name_py:str):
    return f'Hello {name_py}'
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [ProcedureArg(1, ColumnType.VARCHAR, "Tullebukk")])

    try:
        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Hello Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_call_anonymous_procedure_multiple_args(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_call_anonymous_procedure_multiple_args"""
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure(greeting VARCHAR, name VARCHAR)
    returns VARCHAR not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session, greeting_py:str, name_py:str):
    return f'{greeting_py} {name_py}'
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [
        ProcedureArg(1, ColumnType.VARCHAR, "Oh hello"),
        ProcedureArg(1, ColumnType.VARCHAR, "Tullebukk"),
    ])

    try:
        ### Act ###
        res = flake.execute(proc_exec)
        ### Assert ###
        assert res == "Oh hello Tullebukk"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
