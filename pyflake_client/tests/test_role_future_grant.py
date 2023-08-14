# -*- coding: utf-8 -*-
import queue
import uuid

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.grant_action import GrantAction
from pyflake_client.models.assets.grants.database_object_future_grant import (
    DatabaseObjectFutureGrant,
)
from pyflake_client.models.assets.grants.schema_object_future_grant import (
    SchemaObjectFutureGrant,
)
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.describables.future_grant import (
    FutureGrant as FutureGrantDescribable,
)
from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.entities.future_grant import FutureGrant as FutureGrantEntity
from pyflake_client.models.enums.object_type import ObjectType
from pyflake_client.models.enums.privilege import Privilege


def test_describe_future_grant_for_non_existing_role(flake: PyflakeClient):
    """test_describe_future_grant_for_non_existing_role"""
    ### Act ###
    grants = flake.describe_async(
        describable=FutureGrantDescribable(principal=RoleDescribable(name="NON_EXISTING_ROLE"))
    ).deserialize_many(FutureGrantEntity)

    ### Assert ###
    assert grants == []


def test_role_database_object_future_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_role_database_object_future_grant"""
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    grant = GrantAction(
        role,
        DatabaseObjectFutureGrant(database_name=database.db_name, grant_target=ObjectType.TABLE),
        [Privilege.SELECT, Privilege.REFERENCES],
    )

    try:
        w1 = flake.register_asset_async(database, assets_queue)
        w2 = flake.register_asset_async(role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=FutureGrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(FutureGrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        select = next((r for r in grants if r.privilege == Privilege.SELECT), None)
        assert select is not None
        assert select.grant_on == ObjectType.TABLE
        assert select.grantee_identifier == role.name
        assert select.grantee_type == "ROLE"

        references = next((r for r in grants if r.privilege == Privilege.REFERENCES), None)
        assert references is not None
        assert references.grant_on == ObjectType.TABLE
        assert references.grantee_identifier == role.name
        assert references.grantee_type == "ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_role_schema_object_future_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    ### Arrange ###
    snowflake_comment: str = f"pyflake_client_test_{uuid.uuid4()}"
    database = DatabaseAsset("IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    schema = SchemaAsset(database.db_name, "IGT_DEMO", snowflake_comment, owner=RoleAsset("SYSADMIN"))
    role = RoleAsset("IGT_CREATE_ROLE", snowflake_comment, RoleAsset("USERADMIN"))
    grant = GrantAction(
        role,
        SchemaObjectFutureGrant(
            database_name=database.db_name, schema_name=schema.schema_name, grant_target=ObjectType.TABLE
        ),
        [Privilege.SELECT, Privilege.REFERENCES],
    )
    try:
        w1 = flake.register_asset_async(database, assets_queue)
        w2 = flake.register_asset_async(role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(schema, assets_queue).wait()
        flake.register_asset_async(grant, assets_queue).wait()

        ### Act ###
        grants = flake.describe_async(
            describable=FutureGrantDescribable(principal=RoleDescribable(name=role.name))
        ).deserialize_many(FutureGrantEntity)

        ### Assert ###
        assert grants is not None
        assert len(grants) == 2

        select = next((r for r in grants if r.privilege == Privilege.SELECT), None)
        assert select is not None
        assert select.grant_on == ObjectType.TABLE
        assert select.grantee_identifier == role.name
        assert select.grantee_type == "ROLE"

        references = next((r for r in grants if r.privilege == Privilege.REFERENCES), None)
        assert references is not None
        assert references.grant_on == ObjectType.TABLE
        assert references.grantee_identifier == role.name
        assert references.grantee_type == "ROLE"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
