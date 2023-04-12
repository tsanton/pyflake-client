"""test_database"""
import queue
import uuid

from pyflake_client.client import PyflakeClient

from pyflake_client.models.assets.database import Database as DatabaseAsset
from pyflake_client.models.assets.grants.schema_grant import SchemaGrant
from pyflake_client.models.assets.schema import Schema as SchemaAsset
from pyflake_client.models.assets.role import Role as RoleAsset
from pyflake_client.models.assets.grants.database_object_future_grant import DatabaseObjectFutureGrant
from pyflake_client.models.assets.grants.schema_object_future_grant import SchemaObjectFutureGrant
from pyflake_client.models.assets.grant_action import GrantAction

from pyflake_client.models.describables.role import Role as RoleDescribable
from pyflake_client.models.describables.future_grant import FutureGrant as FutureGrantDescribable

from pyflake_client.models.entities.future_grant import FutureGrant as FutureGrantEntity

from pyflake_client.models.enums.privilege import Privilege
from pyflake_client.models.enums.object_type import ObjectType

def test_describe_future_grant_for_non_existing_role(flake: PyflakeClient):
    """test_describe_grant_for_non_existing_role"""
    ### Act ###
    grants = flake.describe_many(describable=FutureGrantDescribable(principal=RoleDescribable(name="NON_EXISTING_ROLE")), entity=FutureGrantEntity)

    ### Assert ###
    assert grants is None


def test_role_database_object_future_grant(flake: PyflakeClient, assets_queue: queue.LifoQueue):
    ### Arrange ###
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    role = RoleAsset("IGT_CREATE_ROLE", RoleAsset("USERADMIN"), f"pyflake_client_test_{uuid.uuid4()}")
    grant = GrantAction(role, DatabaseObjectFutureGrant(database_name=database.db_name, grant_target=ObjectType.TABLE), [Privilege.SELECT, Privilege.REFERENCES])

    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(grant, assets_queue)

        ### Act ###
        grants = flake.describe_many(describable=FutureGrantDescribable(principal=RoleDescribable(name=role.name)), entity=FutureGrantEntity)

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
    database = DatabaseAsset("IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    schema = SchemaAsset(database, "IGT_DEMO", f"pyflake_client_test_{uuid.uuid4()}", owner=RoleAsset("SYSADMIN"))
    role = RoleAsset("IGT_CREATE_ROLE", RoleAsset("USERADMIN"), f"pyflake_client_test_{uuid.uuid4()}")
    grant = GrantAction(role, SchemaObjectFutureGrant(database_name=database.db_name, schema_name=schema.schema_name, grant_target=ObjectType.TABLE), [Privilege.SELECT, Privilege.REFERENCES])
    try:
        flake.register_asset(database, assets_queue)
        flake.register_asset(schema, assets_queue)
        flake.register_asset(role, assets_queue)
        flake.register_asset(grant, assets_queue)

        ### Act ###
        grants = flake.describe_many(describable=FutureGrantDescribable(principal=RoleDescribable(name=role.name)), entity=FutureGrantEntity)

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