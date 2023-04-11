"""test_descendant_roles"""
# pylint: disable=line-too-long
# pylint: disable=too-many-locals

from pyflake_client.client import PyflakeClient

from pyflake_client.models.entities.role_descendants import (
    RoleDescendants as RoleDescendantsEntity,
)
from pyflake_client.models.describables.role_descendants import (
    RoleDescendants as RoleDescendantsDescribable,
)


def test_get_descendant_roles(flake: PyflakeClient):
    """test_get_descendant_roles: we know that ACCOUNTADMIN is the parent of both SECURITYADMIN and SYSADMIN"""
    ### Act ###
    hierarchy = flake.describe_one(
        RoleDescendantsDescribable("ACCOUNTADMIN"), RoleDescendantsEntity
    )

    assert hierarchy is not None
    sec_admin = next(
        (r for r in hierarchy.descendant_roles if r.role_name == "SECURITYADMIN"), None
    )
    sys_admin = next(
        (r for r in hierarchy.descendant_roles if r.role_name == "SYSADMIN"), None
    )

    ### Assert ###
    assert hierarchy.name == "ACCOUNTADMIN"
    assert sec_admin is not None
    assert (
        sec_admin.distance_from_source == 0
    )  # SECURITYADMIN is a direct child of ACCOUNTADMIN
    assert sec_admin.role_name == "SECURITYADMIN"
    assert sec_admin.parent_role_name == "ACCOUNTADMIN"

    assert sys_admin is not None
    assert (
        sys_admin.distance_from_source == 0
    )  # SYSADMIN is a direct child of ACCOUNTADMIN
    assert sys_admin.role_name == "SYSADMIN"
    assert sys_admin.parent_role_name == "ACCOUNTADMIN"
