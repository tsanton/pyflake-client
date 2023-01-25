"""role_future_grant"""
# pylint: disable=consider-using-f-string
# pylint: disable=line-too-long
from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable


@dataclass(frozen=True)
class RoleFutureGrant(ISnowflakeDescribable):
    """RoleFutureGrant"""
    role_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with show_future_grants_to_role as procedure(role_name varchar)
	returns variant not null
	language python
	runtime_version = '3.8'
	packages = ('snowflake-snowpark-python')
	handler = 'show_future_grants_to_role_py'
as '
def show_future_grants_to_role_py(snowpark_session, role_name_py:str):
	res = []
	for row in snowpark_session.sql(f"SHOW FUTURE GRANTS TO ROLE {role_name_py.upper()}").to_local_iterator():
		res.append(row.as_dict())
	return {
		"role_name": role_name_py,
		"grants": res
	}
'
call show_future_grants_to_role('%(str1)s');
        """ % {"str1": self.role_name}

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        """get_dacite_config"""
        return None
