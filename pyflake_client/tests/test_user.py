# -*- coding: utf-8 -*-
import os
import queue
from datetime import date

from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.role_inheritance import (
    RoleInheritance as RoleInheritanceAsset,
)
from pyflake_client.models.assets.user import User as UserAsset
from pyflake_client.models.describables.user import User as UserDescribable
from pyflake_client.models.describables.user_grant import (
    UserGrant as UserGrantDescribable,
)
from pyflake_client.models.entities.user import User as EntitiesUser
from pyflake_client.models.entities.user_grant import UserGrant as UserGrantEntity


def test_create_user(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    ### Arrange ###
    user = UserAsset(
        name=f"PYFLAKE_CLIENT_TEST_USER_{rand_str}",
        owner=RoleAsset("USERADMIN"),
        comment=comment,
    )

    try:
        flake.register_asset_async(user, assets_queue).wait()

        ### Act ###
        sf_user = flake.describe_async(UserDescribable(user.name)).deserialize_one(EntitiesUser)
        ### Assert ###
        assert sf_user is not None
        assert sf_user.name == user.name
        assert sf_user.comment == user.comment
        assert sf_user.owner == "USERADMIN"
        assert sf_user.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_user(flake: PyflakeClient):
    ### Act ###
    user_id = os.environ.get("SNOWFLAKE_UID")
    assert user_id is not None
    user = flake.describe_async(UserDescribable(user_id)).deserialize_one(EntitiesUser)

    ### Assert ###
    assert user is not None
    assert user.name == user_id


def test_get_user_that_does_not_exist(flake: PyflakeClient):
    ### Act ###
    user = flake.describe_async(UserDescribable("I_SURELY_DO_NOT_EXIST")).deserialize_one(EntitiesUser)

    ### Assert ###
    assert user is None


def test_user_zero_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name=f"PYFLAKE_CLIENT_TEST_USER_{rand_str}",
        owner=user_admin,
        comment=comment,
    )
    try:
        flake.register_asset_async(user, assets_queue).wait()

        ### Act ###
        user_grants = flake.describe_async(
            describable=UserGrantDescribable(
                principal=UserDescribable(user.name),
            )
        ).deserialize_many(UserGrantEntity)
        ### Assert ###
        assert user_grants == []
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_user_with_role_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str):
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name=f"PYFLAKE_CLIENT_TEST_USER_{rand_str}",
        owner=user_admin,
        comment=comment,
    )
    role = RoleAsset(
        name=user.name,
        owner=user_admin,
    )
    inheritance = RoleInheritanceAsset(child_principal=role, parent_principal=user)
    try:
        w1 = flake.register_asset_async(user, assets_queue)
        w2 = flake.register_asset_async(role, assets_queue)
        flake.wait_all([w1, w2])
        flake.register_asset_async(inheritance, assets_queue).wait

        ### Act ###
        user_grants = flake.describe_async(
            describable=UserGrantDescribable(
                principal=UserDescribable(user.name),
            )
        ).deserialize_many(UserGrantEntity)

        ### Assert ###
        assert user_grants is not None
        assert len(user_grants) == 1
        role_grant = user_grants[0]
        assert role_grant.grantee_identifier == user.name
        assert role_grant.grantee_type == "USER"
        assert role_grant.granted_identifier == role.name

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_user_with_multiple_role_grants(
    flake: PyflakeClient, assets_queue: queue.LifoQueue, rand_str: str, comment: str
):
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name=f"PYFLAKE_CLIENT_TEST_USER_{rand_str}",
        owner=user_admin,
        comment=comment,
    )
    role_1 = RoleAsset(
        name=f"PYFLAKE_CLIENT_TEST_ROLE_1_{rand_str}",
        owner=user_admin,
    )
    role_2 = RoleAsset(
        name=f"PYFLAKE_CLIENT_TEST_ROLE_2_{rand_str}",
        owner=user_admin,
    )
    inheritance_1 = RoleInheritanceAsset(child_principal=role_1, parent_principal=user)
    inheritance_2 = RoleInheritanceAsset(child_principal=role_2, parent_principal=user)
    try:
        w1 = flake.register_asset_async(user, assets_queue)
        w2 = flake.register_asset_async(role_1, assets_queue)
        w3 = flake.register_asset_async(role_2, assets_queue)
        flake.wait_all([w1, w2, w3])
        w4 = flake.register_asset_async(inheritance_1, assets_queue)
        w5 = flake.register_asset_async(inheritance_2, assets_queue)
        flake.wait_all([w4, w5])

        ### Act ###
        user_grants = flake.describe_async(
            describable=UserGrantDescribable(
                principal=UserDescribable(user.name),
            )
        ).deserialize_many(UserGrantEntity)

        ### Assert ###
        assert user_grants is not None
        assert len(user_grants) == 2
        role_1_grant = next(g for g in user_grants if g.granted_identifier == role_1.name)
        role_2_grant = next(g for g in user_grants if g.granted_identifier == role_2.name)
        assert role_1_grant.grantee_identifier == user.name
        assert role_1_grant.grantee_type == "USER"
        assert role_2_grant.grantee_identifier == user.name
        assert role_2_grant.grantee_type == "USER"

    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)
