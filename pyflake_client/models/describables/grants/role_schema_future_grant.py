"""role_schema_future_grant"""
from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class RoleSchemaFutureGrant(ISnowflakeDescribable):
    """RoleSchemaFutureGrant"""
    role_name: str
    db_name: str
    schema_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return f"""
with get_role_future_schema_object_grants as procedure(role_name varchar, db_name varchar, schema_name varchar)
  returns variant not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'get_role_future_schema_object_grants_py'
as '
def scoped_privilege(db_name:str, schema_name:str, row):
    try:
        parts = row["name"].upper().split(".")
        if len(parts) == 3 and parts[0] == db_name and parts[1] == schema_name:
            return True, row.as_dict()
    except:
        return False, {{}}
    return False, {{}}

def get_role_future_schema_object_grants_py(snowpark_session, role_name_py: str, db_name_py:str, schema_name_py:str):
  res = {{}}
  try:
      for row in snowpark_session.sql(f"SHOW FUTURE GRANTS TO ROLE {{role_name_py}}").to_local_iterator():
          scoped, data = scoped_privilege(db_name_py, schema_name_py, row)
          if scoped:
              res.setdefault(row["grant_on"], []).append(row["privilege"])
  except:
      pass
  return {{
    "role_name": role_name_py,
    "db_name": db_name_py,
    "schema_name": schema_name_py,
    "future_grants": [{{"grant_target": k, "privileges": v}} for k, v in res.items()] 
  }}
'
call get_role_future_schema_object_grants('{self.role_name}', '{self.db_name}', '{self.schema_name}');
"""

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        return Config(cast=[ObjectType])
