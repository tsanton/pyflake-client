"""test_table"""
# pylint: disable=invalid-name
# pylint: disable=line-too-long
from datetime import date, datetime

from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Column, Table as TableAsset
from pyflake_client.models.assets.table_columns import (
    Varchar,
    Number,
    Integer,
    Identity,
    Timestamp,
    Date,
)
from pyflake_client.models.assets.role import Role as AssetsRole


def test_create_simple_table_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl"""
    ### Arrange ###
    schema = Schema(
        database=db_asset_fixture,
        schema_name="S1",
        comment="SCHEMA",
        owner=AssetsRole("SYSADMIN"),
    )
    table = TableAsset(
        schema,
        "TEST",
        [Integer(name="ID", identity=Identity(1, 1))],
        owner=AssetsRole("SYSADMIN"),
    )

    ### Act ###
    definition = table.get_create_statement()

    assert (
        definition
        == """CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1))"""
    )


def test_create_complex_table_ddl(db_asset_fixture: Database):
    """test_create_complex_table_ddl"""
    ### Arrange ###
    schema = Schema(
        database=db_asset_fixture,
        schema_name="S1",
        comment="SCHEMA",
        owner=AssetsRole("SYSADMIN"),
    )
    table = TableAsset(
        schema,
        "TEST",
        [
            Integer(name="ID", identity=Identity(1, 1)),
            Varchar(name="VARCHAR_NO_DEFAULT"),
            Varchar(name="VARCHAR_DEFAULT", default_value="YES"),
        ],
        owner=AssetsRole("SYSADMIN"),
    )

    ### Act ###
    definition = table.get_create_statement()

    assert (
        definition
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),VARCHAR_NO_DEFAULT VARCHAR (16777216) NOT NULL,VARCHAR_DEFAULT VARCHAR (16777216) NOT NULL DEFAULT 'YES')"
    )


def test_create_complex_table_with_primary_key_ddl(db_asset_fixture: Database):
    """test_create_complex_table_ddl"""
    ### Arrange ###
    schema = Schema(
        database=db_asset_fixture,
        schema_name="S1",
        comment="SCHEMA",
        owner=AssetsRole("SYSADMIN"),
    )
    table = TableAsset(
        schema,
        "TEST",
        [
            Integer(name="ID", identity=Identity(1, 1)),
            Varchar(name="VARCHAR_1", primary_key=True),
            Varchar(
                name="VARCHAR_2",
                primary_key=True,
            ),
        ],
        owner=AssetsRole("SYSADMIN"),
    )

    ### Act ###
    definition = table.get_create_statement()

    assert (
        definition
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),VARCHAR_1 VARCHAR (16777216) NOT NULL,VARCHAR_2 VARCHAR (16777216) NOT NULL, PRIMARY KEY(VARCHAR_1,VARCHAR_2))"
    )


def test_create_simple_table_with_default_date_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATE) values(default);
    """
    ### Arrange ###
    schema = Schema(
        database=db_asset_fixture,
        schema_name="S1",
        comment="SCHEMA",
        owner=AssetsRole("SYSADMIN"),
    )
    table = TableAsset(
        schema,
        "TEST",
        [
            Integer(name="ID", identity=Identity(1, 1)),
            Date(name="SOME_DATE", default_value=date(2000, 1, 1)),
        ],
        owner=AssetsRole("SYSADMIN"),
    )

    ### Act ###
    definition = table.get_create_statement()

    assert (
        definition
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),SOME_DATE DATE NOT NULL DEFAULT '2000-01-01'::date)"
    )


def test_create_simple_table_with_default_datetime_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATETIME) values(default);
    """
    ### Arrange ###
    schema = Schema(
        database=db_asset_fixture,
        schema_name="S1",
        comment="SCHEMA",
        owner=AssetsRole("SYSADMIN"),
    )
    table = TableAsset(
        schema,
        "TEST",
        [
            Integer(name="ID", not_null=True, identity=Identity(1, 1)),
            Timestamp(
                name="SOME_DATETIME",
                default_value=datetime(2000, 1, 1, 0, 0, 0),
                nullable=True,
            ),
        ],
        owner=AssetsRole("SYSADMIN"),
    )

    ### Act ###
    definition = table.get_create_statement()

    assert (
        definition
        == """CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) IDENTITY (1,1),SOME_DATETIME TIMESTAMP(0) DEFAULT '2000-01-01T00:00:00.000000'::TIMESTAMP(0))"""
    )
