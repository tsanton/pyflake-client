"""test_user"""
import os
import queue
import uuid
from datetime import date


from pyflake_client.client import PyflakeClient
from pyflake_client.models.assets.user import User as UserAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.describables.user import User as DescribablesUser
from pyflake_client.models.entities.user import User as EntitiesUser
from pyflake_client.models.assets.role_inheritance import RoleInheritance as RoleInheritanceAsset
from pyflake_client.models.describables.grant import Grant as GrantDescribable
from pyflake_client.models.describables.user import User as UserDescribable
from pyflake_client.models.entities.user_grant import UserGrant as UserGrantEntity


def test_create_user(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_create_role"""
    ### Arrange ###
    user = UserAsset(
        name="IGT_CREATE_USER",
        owner=RoleAsset("USERADMIN"),
        comment=f"pyflake_client_test_{uuid.uuid4()}",
    )

    try:
        flake.register_asset(user, assets_queue)

        ### Act ###
        sf_user = flake.describe_one(DescribablesUser(user.name), EntitiesUser)
        ### Assert ###
        assert sf_user is not None
        assert sf_user.name == user.name
        assert sf_user.comment == user.comment
        assert sf_user.owner == "USERADMIN"
        assert sf_user.created_on is not None
        assert sf_user.created_on.date() == date.today()
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_get_user(flake: PyflakeClient):
    """test_get_role"""
    ### Act ###
    user_id = os.environ.get("SNOWFLAKE_UID")
    assert user_id is not None
    user = flake.describe_one(DescribablesUser(user_id), EntitiesUser)

    ### Assert ###
    assert user is not None
    assert user.name == user_id


def test_get_user_that_does_not_exist(flake: PyflakeClient):
    """test_get_role_that_does_not_exist"""
    ### Act ###
    user = flake.describe_one(DescribablesUser("I_SURELY_DO_NOT_EXIST"), EntitiesUser)

    ### Assert ###
    assert user is None


def test_user_zero_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_user_zero_grants"""
    comment = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name="IGT_CREATE_USER",
        owner=user_admin,
        comment=comment,
    )
    try:
        flake.register_asset(user, assets_queue)

        ### Act ###
        user_grants = flake.describe_many(
            describable=GrantDescribable(
                principal=UserDescribable(user.name),
            ),
            entity=UserGrantEntity,
        )
        ### Assert ###
        assert user_grants == []
    finally:
        ### Cleanup ###
        flake.delete_assets(assets_queue)


def test_user_with_role_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_user_with_role_grant"""
    comment = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name="IGT_CREATE_USER",
        owner=user_admin,
        comment=comment,
    )
    role = RoleAsset(
        name=f"IGT_CREATE_ROLE",
        owner=user_admin,
    )
    inheritance = RoleInheritanceAsset(child_principal=role, parent_principal=user)
    try:
        flake.register_asset(user, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(inheritance, assets_queue)

        ### Act ###
        user_grants = flake.describe_many(
            describable=GrantDescribable(
                principal=UserDescribable(user.name),
            ),
            entity=UserGrantEntity,
        )

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


def test_user_with_multiple_role_grants(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    """test_user_with_multiple_role_grants"""
    comment = f"pyflake_client_test_{uuid.uuid4()}"
    user_admin = RoleAsset("USERADMIN")
    user = UserAsset(
        name="IGT_CREATE_USER",
        owner=user_admin,
        comment=comment,
    )
    role_1 = RoleAsset(
        name=f"IGT_CREATE_ROLE_1",
        owner=user_admin,
    )
    role_2 = RoleAsset(
        name=f"IGT_CREATE_ROLE_2",
        owner=user_admin,
    )
    inheritance_1 = RoleInheritanceAsset(child_principal=role_1, parent_principal=user)
    inheritance_2 = RoleInheritanceAsset(child_principal=role_2, parent_principal=user)
    try:
        flake.register_asset(user, assets_queue)
        flake.register_asset(role_1, assets_queue)
        flake.register_asset(role_2, assets_queue)
        flake.register_asset(inheritance_1, assets_queue)
        flake.register_asset(inheritance_2, assets_queue)

        ### Act ###
        user_grants = flake.describe_many(
            describable=GrantDescribable(
                principal=UserDescribable(user.name),
            ),
            entity=UserGrantEntity,
        )

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
