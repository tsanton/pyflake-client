# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
import os
import queue
import uuid
from typing import Generator

import pytest
import snowflake.connector
from snowflake.connector import SnowflakeConnection

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.role import Role as RoleAsset


# https://docs.pytest.org/en/6.2.x/fixture.html#fixture-scopes
@pytest.fixture(scope="function")
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

    cli = PyflakeClient(conn)

    yield cli

    conn.close()


@pytest.fixture(scope="function")
def assets_queue() -> queue.LifoQueue:
    """assets_queue"""
    return queue.LifoQueue()


@pytest.fixture(scope="function")
def database() -> DatabaseAsset:
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    return database


@pytest.fixture(scope="session")
def comment() -> str:
    return f"pyflake_client_test_{uuid.uuid4()}"
