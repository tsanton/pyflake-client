# -*- coding: utf-8 -*-
import queue
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.database_role import DatabaseRole as DatabaseRoleAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.role_inheritance import (
    RoleInheritance as RoleInheritanceAsset,
)
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.describables.schema import Schema as SchemaDescribable
from pyflake_client.models.entities.schema import Schema as SchemaEntity


def test_get_schema(flake: PyflakeClient):
    ### Act ###
    schema = flake.describe_async(SchemaDescribable("INFORMATION_SCHEMA", "SNOWFLAKE")).deserialize_one(SchemaEntity)

    ### Assert ###
    assert schema is not None
    assert schema.name == "INFORMATION_SCHEMA"
    assert schema.database_name == "SNOWFLAKE"


def test_get_schema_when_database_does_not_exist(flake: PyflakeClient):
    ### Act ###
    schema = flake.describe_async(SchemaDescribable("INFORMATION_SCHEMA", "THIS_DB_DOES_NOT_EXIST")).deserialize_one(
        SchemaEntity
    )

    ### Assert ###
    assert schema is None


def test_get_schema_when_schema_does_not_exist(flake: PyflakeClient):
    ### Act ###
    schema = flake.describe_async(SchemaDescribable("THIS_SCHEMA_DOES_NOT_EXIST", "SNOWFLAKE")).deserialize_one(
        SchemaEntity
    )

    ### Assert ###
    assert schema is None


def test_create_schema_with_role_owner(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=RoleAsset("SYSADMIN"))
    sch = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=RoleAsset("SYSADMIN"),
    )
    try:
        flake.register_asset_async(database, assets_queue).wait()
        flake.create_asset_async(sch).wait()

        ### Act ###
        sch_so = flake.describe_async(
            SchemaDescribable(sch.schema_name, sch.db_name),
        ).deserialize_one(SchemaEntity)

        ### Assert ###
        assert sch_so is not None
        assert sch_so.name == sch.schema_name
        assert sch_so.database_name == sch.db_name
        assert sch_so.comment == sch.comment
        assert sch_so.owner == "SYSADMIN"
        assert sch_so.retention_time == 1
        assert sch_so.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_schema_with_database_role_owner(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    ### Arrange ###
    sys_admin = RoleAsset("SYSADMIN")
    database = DatabaseAsset(f"PYFLAKE_CLIENT_TEST_DB_{rand_str}", comment, owner=sys_admin)
    db_role = DatabaseRoleAsset(
        name="PYFLAKE_CLIENT_TEST_DB_ROLE",
        database_name=database.db_name,
        comment=comment,
        owner=RoleAsset("USERADMIN"),
    )
    rel = RoleInheritanceAsset(
        child_principal=db_role, parent_principal=sys_admin
    )  # So we can delete the schema in the finally

    schema = SchemaAsset(
        db_name=database.db_name,
        schema_name="TEST_SCHEMA",
        comment=comment,
        owner=db_role,
    )

    try:
        flake.register_asset_async(database, assets_queue).wait()
        w1 = flake.create_asset_async(db_role)
        w2 = flake.create_asset_async(schema)
        flake.wait_all([w1, w2])
        flake.create_asset_async(rel).wait()

        ### Act ###
        sch = flake.describe_async(SchemaDescribable(schema.schema_name, database.db_name)).deserialize_one(
            SchemaEntity
        )

        ### Assert ###
        assert sch is not None
        assert sch.name == schema.schema_name
        assert sch.database_name == schema.db_name
        assert sch.comment == schema.comment
        assert sch.owner == db_role.name
        assert sch.created_on.date() == date.today()

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
