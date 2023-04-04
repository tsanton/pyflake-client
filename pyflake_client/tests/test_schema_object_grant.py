"""test_role_schema_object_privilege"""
import uuid
import queue

from pyflake_client.models.assets.grant import Grant as GrantAsset
from pyflake_client.models.assets.grants.role_schema_future_grant import (
    RoleSchemaFutureGrant as RoleSchemaFutureGrantAsset,
)
from pyflake_client.models.assets.table_columns import Identity, Integer, Varchar
from pyflake_client.models.assets.role import Role as AssetsRole
from pyflake_client.models.describables.grant import (
    Grant as DescribablesGrant,
)
from pyflake_client.models.entities.grant import (
    Grant as EntitiesGrant,
)
from pyflake_client.tests.utilities import _spawn_database_and_schema
from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.table import Table as AssetsTable
from pyflake_client.models.describables.table import Table as DescribablesTable
from pyflake_client.models.assets.role_relationship import RoleRelationship
from pyflake_client.models.assets.role import Role
from pyflake_client.models.enums.object_type import ObjectType


def test_create_table_without_future_privileges(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_create_table_without_future_privileges"""
    ### Arrange ###
    d, s1, _ = _spawn_database_and_schema(flake, assets_queue)
    table_columns = [
        Integer(name="ID", identity=Identity(1, 1)),
        Varchar(name="SOME_VARCHAR", primary_key=True),
    ]
    t = AssetsTable(s1, "TEST", table_columns, owner=AssetsRole("SYSADMIN"))
    try:
        flake.register_asset(t, assets_queue)

        ### Act ###
        desc_table = DescribablesTable(
            database_name=d.db_name, schema_name=s1.schema_name, name=t.table_name
        )
        showable = DescribablesGrant(principal=desc_table)
        tp = flake.describe_one(showable, EntitiesGrant)

        ### Assert ###
        assert tp is not None
        assert tp.privilege == "OWNERSHIP"
        assert tp.granted_by == "ACCOUNTADMIN"
        assert tp.granted_on == "TABLE"
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_create_table_with_future_privileges(
    flake: PyflakeClient, assets_queue: queue.LifoQueue
):
    """test_create_table_with_future_privileges"""
    ### Arrange ###
    d, s1, _ = _spawn_database_and_schema(flake, assets_queue)
    role: Role = Role(
        f"{d.db_name}_{s1.schema_name}_ACCESS_ROLE_R",
        Role("USERADMIN"),
        f"pyflake_client_TEST_{uuid.uuid4()}",
    )
    role_privileges = ["SELECT", "REFERENCES", "OWNERSHIP"]
    role_grant = GrantAsset(
        RoleSchemaFutureGrantAsset(
            role.name, d.db_name, s1.schema_name, ObjectType.TABLE
        ),
        role_privileges,
    )
    role_relationship = RoleRelationship(role.name, "SYSADMIN")
    table_columns = [
        Integer(name="ID", identity=Identity(1, 1)),
        Varchar(name="SOME_VARCHAR", primary_key=True),
    ]
    t = AssetsTable(s1, "TEST", table_columns, owner=AssetsRole("SYSADMIN"))

    try:
        flake.register_asset(role, assets_queue)
        flake.register_asset(role_relationship, assets_queue)
        flake.register_asset(role_grant, assets_queue)
        flake.register_asset(t, assets_queue)

        ### Act ###
        desc_table = DescribablesTable(
            database_name=d.db_name, schema_name=s1.schema_name, name=t.table_name
        )
        showable = DescribablesGrant(principal=desc_table)
        grants = flake.describe_many(showable, EntitiesGrant)

        ### Assert ###
        assert grants is not None
        ownership_grant = next(g for g in grants if g.privilege == "OWNERSHIP")
        references_grant = next(g for g in grants if g.privilege == "REFERENCES")
        select_grant = next(g for g in grants if g.privilege == "SELECT")
        assert ownership_grant is not None
        assert references_grant is not None
        assert select_grant is not None
        assert ownership_grant.granted_by == role.name
        assert references_grant.granted_by == role.name
        assert select_grant.granted_by == role.name
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
