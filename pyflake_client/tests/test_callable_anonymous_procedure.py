# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=too-many-locals

import json
from dataclasses import dataclass

from pyflake_client.client import PyflakeClient
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.executables.anonymous_procedure import (
    AnonymousProcedure as AnonymousProcedureExec,
)
from pyflake_client.models.executables.procedure_arg import ProcedureArg
from pyflake_client.tests.utilities import Plo, compare


def test_call_anonymous_procedure_one_arg(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure(name VARCHAR)
    returns VARCHAR not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session, name_py:str):
    return f'Hello {name_py}'
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [ProcedureArg(1, ColumnType.VARCHAR, "Tullebukk")])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(str, lambda row: row)
    ### Assert ###
    assert res == "Hello Tullebukk"


def test_call_anonymous_procedure_multiple_args(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure(greeting VARCHAR, name VARCHAR)
    returns VARCHAR not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session, greeting_py:str, name_py:str):
    return f'{greeting_py} {name_py}'
$$
    """
    proc_exec = AnonymousProcedureExec(
        "anonymous_procedure",
        sql,
        [
            ProcedureArg(1, ColumnType.VARCHAR, "Oh hello"),
            ProcedureArg(1, ColumnType.VARCHAR, "Tullebukk"),
        ],
    )

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(str, lambda row: row)

    ### Assert ###
    assert res == "Oh hello Tullebukk"


def test_call_anonymous_procedure_fetch_one_varchar(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARCHAR not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return 'Hello you!'
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(str, lambda row: row)
    ### Assert ###
    assert res == "Hello you!"


def test_call_anonymous_procedure_fetch_one_bool(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns BOOLEAN not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return True
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(bool, lambda row: bool(row))
    ### Assert ###
    assert res is True


def test_call_anonymous_procedure_fetch_one_integer(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns NUMBER(38,0) not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return 12
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(int, lambda row: int(row))
    ### Assert ###
    assert res == 12


def test_call_anonymous_procedure_fetch_one_number(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns NUMBER(38,4) not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return 12.1234
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(float, lambda row: float(round(row, 4)))
    ### Assert ###
    assert res == 12.1234


def test_call_anonymous_procedure_fetch_one_simple_dict(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return {"foo": "bar", "bar":"baz"}
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(dict[str, str], lambda row: json.loads(row))
    ### Assert ###
    assert res is not None
    assert "foo" in res
    assert res["foo"] == "bar"
    assert "bar" in res
    assert res["bar"] == "baz"


def test_call_anonymous_procedure_fetch_one_class(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return {"foo": "bar", "bar":"baz"}
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    @dataclass
    class Anon:
        foo: str
        bar: str

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_one(Anon, lambda row: Anon(**json.loads(row)))
    ### Assert ###
    assert res is not None
    res.foo = "bar"
    res.bar = "baz"


def test_call_anonymous_procedure_fetch_many_varchars(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return ["foo", "bar"]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(str, lambda row: row)
    ### Assert ###
    assert compare(["foo", "bar"], res)


def test_call_anonymous_procedure_fetch_many_bools(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return [True, False, True, False]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(bool, lambda row: bool(row))
    ### Assert ###
    assert compare([True, False, True, False], res)


def test_call_anonymous_procedure_fetch_many_integers(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return [10, 11, 12]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(int, lambda row: int(row))
    ### Assert ###
    assert compare([10, 11, 12], res)


def test_call_anonymous_procedure_fetch_many_numbers(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return [10.123, 11.234, 12.345]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(float, lambda row: float(round(row, 3)))
    ### Assert ###
    assert compare([10.123, 11.234, 12.345], res)


def test_call_anonymous_procedure_fetch_many_simple_dicts(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return [{"foo": "bar1", "bar":"baz1"}, {"foo": "bar2", "bar":"baz2"}]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(dict[str, str], lambda row: row)

    ### Assert ###
    expected = [{"foo": "bar1", "bar": "baz1"}, {"foo": "bar2", "bar": "baz2"}]

    assert all(Plo.contains_by(res, lambda x: x == item) for item in expected)
    assert all(Plo.contains_by(expected, lambda x: x == item) for item in res)


def test_call_anonymous_procedure_fetch_many_classes(flake: PyflakeClient):
    ### Arrange ###
    sql: str = """
with anonymous_procedure AS procedure()
    returns VARIANT not null
    language python
    runtime_version = '3.10'
    packages = ('snowflake-snowpark-python')
    handler = 'main_py'
    as $$
def main_py(snowpark_session):
    return [{"foo": "bar1", "bar":"baz1"}, {"foo": "bar2", "bar":"baz2"}]
$$
    """
    proc_exec = AnonymousProcedureExec("anonymous_procedure", sql, [])

    @dataclass
    class Anon:
        foo: str
        bar: str

    ### Act ###
    res = flake.execute_async(proc_exec.get_call_statement()).fetch_many(Anon, lambda row: Anon(**row))
    ### Assert ###
    expected = [Anon(foo="bar1", bar="baz1"), Anon(foo="bar2", bar="baz2")]

    assert all(Plo.contains_by(res, lambda x: x == item) for item in expected)
    assert all(Plo.contains_by(expected, lambda x: x == item) for item in res)
