"""conftest"""
# pylint: disable=line-too-long
import os
import queue

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from pyflake_client.client import PyflakeClient


# https://docs.pytest.org/en/6.2.x/fixture.html#fixture-scopes
@pytest.fixture(scope="session")
def flake() -> None:
    """flake"""

    conn: SnowflakeConnection = snowflake.connector.connect(
        host=os.getenv("SNOWFLAKE_HOST"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_UID"),
        password=os.getenv("SNOWFLAKE_PWD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WH"),
        autocommit=True
    )

    cli = PyflakeClient(conn, "IGT_DEMO", "MGMT")

    yield cli

    conn.close()


@pytest.fixture(scope="function")
def assets_queue() -> None:
    """assets_queue"""
    return queue.LifoQueue()
