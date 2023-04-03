from dataclasses import dataclass

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.describables.snowflake_grant_principal import (
    ISnowflakeGrantPrincipal,
)

from pyflake_client.models.assets.database_role import (
    DatabaseRole as AssetsDatabaseRole,
)
from pyflake_client.models.assets.role import Role as AssetsRole


@dataclass
class Grant(ISnowflakeDescribable):
    principal: ISnowflakeGrantPrincipal

    def get_describe_statement(self) -> str:
        if isinstance(self.principal, AssetsRole):
            principal_type = "ROLE"  # TODO: enum for this
            principal_identifier = self.principal.name
        elif isinstance(self.principal, AssetsDatabaseRole):
            principal_type = "DATABASE_ROLE"  # TODO: enum for this
            principal_identifier = (
                f"{self.principal.database_name}.{self.principal.name}"
            )
        else:
            raise NotImplementedError(
                f"Grant describe statement for {self.__class__} is not implemented"
            )

        query = """
with show_grants_to_principal as procedure(principal_type varchar, principal_identifier varchar)
    returns variant not null
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python')
    handler = 'show_grants_to_principal_py'
as $$
def show_grants_to_principal_py(snowpark_session, principal_type_py:str, principal_identifier_py:str):
    res = []
    for row in snowpark_session.sql(f'SHOW GRANTS TO {principal_type_py} {principal_identifier_py}').to_local_iterator():
        res.append(row.as_dict())
    return res
$$
call show_grants_to_principal('%(s1)s', '%(s2)s');
        """ % {
            "s1": principal_type,
            "s2": principal_identifier,
        }
        return query
