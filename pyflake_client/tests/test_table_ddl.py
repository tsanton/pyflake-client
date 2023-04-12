"""test_table"""
from datetime import date, datetime
import uuid

from pyflake_client.models.assets.schema import Schema
from pyflake_client.models.assets.table import Table as TableAsset
from pyflake_client.models.assets.table_columns import (
    Varchar,
    Integer,
    Identity,
    Timestamp,
    Date,
)
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.role import Role as AssetsRole


def test_create_simple_table_ddl():
    """test_create_simple_table_ddl"""
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
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
        == """CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1));"""
    )


def test_create_complex_table_ddl():
    """test_create_complex_table_ddl"""
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
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
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),VARCHAR_NO_DEFAULT VARCHAR (16777216) NOT NULL,VARCHAR_DEFAULT VARCHAR (16777216) NOT NULL DEFAULT 'YES');"
    )


def test_create_complex_table_with_primary_key_ddl():
    """test_create_complex_table_ddl"""
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
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
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),VARCHAR_1 VARCHAR (16777216) NOT NULL,VARCHAR_2 VARCHAR (16777216) NOT NULL, PRIMARY KEY(VARCHAR_1,VARCHAR_2));"
    )


def test_create_simple_table_with_default_date_ddl():
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATE) values(default);
    """
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
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
        == "CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) NOT NULL IDENTITY (1,1),SOME_DATE DATE NOT NULL DEFAULT '2000-01-01'::date);"
    )


def test_create_simple_table_with_default_datetime_ddl():
    """test_create_simple_table_ddl
    insert into <DB>.<SCHEMA>.TEST (SOME_DATETIME) values(default);
    """
    ### Arrange ###
    database = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    schema = Schema(
        database=database,
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
        == """CREATE OR REPLACE TABLE IGT_DEMO.S1.TEST (ID NUMBER(38,0) IDENTITY (1,1),SOME_DATETIME TIMESTAMP(0) DEFAULT '2000-01-01T00:00:00.000000'::TIMESTAMP(0));"""
    )
