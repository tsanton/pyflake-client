"""test_table"""
# pylint: disable=invalid-name
# pylint: disable=line-too-long
from datetime import date, datetime

from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.assets.database import Database
from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Column, Table as TableAsset
from pyflake_client.models.assets.role import Role as AssetsRole

def test_create_simple_table_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl"""
    ### Arrange ###
    schema = Schema(database=db_asset_fixture, schema_name="S1", comment="SCHEMA", owner=AssetsRole("SYSADMIN"))
    table = TableAsset(schema, "TEST", [Column("ID", ColumnType.INTEGER, identity=True)])

    ### Act ###
    definition = table.get_create_statement()

    assert definition == """create or replace table IGT_DEMO.S1.TEST(ID INTEGER IDENTITY (1,1) NOT NULL)"""


def test_create_complex_table_ddl(db_asset_fixture: Database):
    """test_create_complex_table_ddl"""
    ### Arrange ###
    schema = Schema(database=db_asset_fixture, schema_name="S1", comment="SCHEMA", owner=AssetsRole("SYSADMIN"))
    table = TableAsset(schema, "TEST", [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("VARCHAR_NO_DEFAULT", ColumnType.VARCHAR),
        Column("VARCHAR_DEFAULT", ColumnType.VARCHAR, default_value="YES"),
    ])

    ### Act ###
    definition = table.get_create_statement()

    assert definition == "create or replace table IGT_DEMO.S1.TEST(ID INTEGER IDENTITY (1,1) NOT NULL, VARCHAR_NO_DEFAULT VARCHAR(16777216) NOT NULL, VARCHAR_DEFAULT VARCHAR(16777216) NOT NULL DEFAULT 'YES')"


def test_create_complex_table_with_primary_key_ddl(db_asset_fixture: Database):
    """test_create_complex_table_ddl"""
    ### Arrange ###
    schema = Schema(database=db_asset_fixture, schema_name="S1", comment="SCHEMA", owner=AssetsRole("SYSADMIN"))
    table = TableAsset(schema, "TEST", [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("VARCHAR_NO_DEFAULT", ColumnType.VARCHAR, primary_key=True),
        Column("VARCHAR_DEFAULT", ColumnType.VARCHAR, primary_key=True),
    ])

    ### Act ###
    definition = table.get_create_statement()

    assert definition == "create or replace table IGT_DEMO.S1.TEST(ID INTEGER IDENTITY (1,1) NOT NULL, VARCHAR_NO_DEFAULT VARCHAR(16777216) NOT NULL, VARCHAR_DEFAULT VARCHAR(16777216) NOT NULL, PRIMARY KEY(VARCHAR_NO_DEFAULT,VARCHAR_DEFAULT))"


def test_create_simple_table_with_default_date_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATE) values(default);
    """
    ### Arrange ###
    schema = Schema(database=db_asset_fixture, schema_name="S1", comment="SCHEMA", owner=AssetsRole("SYSADMIN"))
    table = TableAsset(schema, "TEST", [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_DATE", ColumnType.DATE, default_value=date(2000, 1, 1)),
    ])

    ### Act ###
    definition = table.get_create_statement()

    assert definition == """create or replace table IGT_DEMO.S1.TEST(ID INTEGER IDENTITY (1,1) NOT NULL, SOME_DATE DATE NOT NULL DEFAULT '2000-01-01'::date)"""


def test_create_simple_table_with_default_datetime_ddl(db_asset_fixture: Database):
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATETIME) values(default);
    """
    ### Arrange ###
    schema = Schema(database=db_asset_fixture, schema_name="S1", comment="SCHEMA", owner=AssetsRole("SYSADMIN"))
    table = TableAsset(schema, "TEST", [
        Column("ID", ColumnType.INTEGER, identity=True),
        Column("SOME_DATETIME", ColumnType.TIMESTAMP, default_value=datetime(2000, 1, 1)),
    ])

    ### Act ###
    definition = table.get_create_statement()

    assert definition == """create or replace table IGT_DEMO.S1.TEST(ID INTEGER IDENTITY (1,1) NOT NULL, SOME_DATETIME TIMESTAMP_NTZ(2) NOT NULL DEFAULT '2000-01-01T00:00:00.000000'::TIMESTAMP_NTZ(2))"""
