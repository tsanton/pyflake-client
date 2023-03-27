"""conftest"""
# pylint: disable=line-too-long
import os
import queue
import uuid
from typing import Generator

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as AssetsDatabase
from pyflake_client.models.assets.role import Role as AssetsRole


# https://docs.pytest.org/en/6.2.x/fixture.html#fixture-scopes
@pytest.fixture(scope="session")
def flake() -> Generator:
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

    cli = PyflakeClient(conn, "IGT_DEMO", "MGMT")

    yield cli

    conn.close()


@pytest.fixture(scope="function")
def assets_queue() -> queue.LifoQueue:
    """assets_queue"""
    return queue.LifoQueue()


@pytest.fixture(scope="function")
def db_asset_fixture() -> AssetsDatabase:
    database: AssetsDatabase = AssetsDatabase(
        "IGT_DEMO", f"pyflake_client_TEST_{uuid.uuid4()}", owner=AssetsRole("SYSADMIN")
    )
    return database


@pytest.fixture(scope="session")
def existing_database() -> str:
    """return a database we are certain exists"""
    return "SNOWFLAKE"


@pytest.fixture(scope="session")
def existing_database_role() -> str:
    """return a database role that we are certain exists in SNOWFLAKE database"""
    return "ALERT_VIEWER"
