"""role_schema_object_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass

from dacite import Config
from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class RoleSchemaObjectGrant(ISnowflakeDescribable):
    """RoleSchemaObjectGrant"""
    role_name: str
    db_name: str
    schema_name: str
    object_type: ObjectType
    object_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with get_role_privileges_on_object as procedure(role_name varchar, db_name varchar, schema_name varchar, object_type varchar, object_name varchar)
  returns variant not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'main_py'
as '
def get_privileges_on_object_py(snowpark_session, db_name:str, schema_name:str, object_type:str, object_name: str):
  res = {}
  try:
      for row in snowpark_session.sql(f"SHOW GRANTS ON {object_type} {db_name}.{schema_name}.{object_name}").to_local_iterator():
          if row["granted_to"] == "ROLE":
              res.setdefault(row["grantee_name"], []).append(row["privilege"])
  except:
      pass
  return [{"role_name": k, "privileges": v} for k, v in res.items()]
  
def is_role_in_hierarchy_py(snowpark_session, role_name:str, parent_role_name:str, shown) -> bool:
    if role_name == parent_role_name:
        return True
    if role_name not in shown:
        shown.append(role_name)
        any_inherited = []
        for row in snowpark_session.sql(f"SHOW GRANTS OF ROLE {role_name}").to_local_iterator():
            if row["granted_to"] == "ROLE":
                any_inherited.append(is_role_in_hierarchy_py(snowpark_session, row["grantee_name"], parent_role_name, shown))
        if any(any_inherited):
            return True
    return False

def main_py(snowpark_session, role_name_py:str, db_name_py:str, schema_name_py:str, object_type_py:str, object_name_py: str):
    resp = {
        "role_name": role_name_py,
        "db_name": db_name_py,
        "schema_name": schema_name_py,
        "object_type": object_type_py,
        "object_name": object_name_py,
        "granted_privileges": [],
        "inherited_privileges": [],
        "all_privileges": set()
      }
    table_grants = get_privileges_on_object_py(snowpark_session, db_name_py, schema_name_py, object_type_py, object_name_py)
    for grant in table_grants:
        if grant["role_name"] == role_name_py:
            resp["granted_privileges"] = grant["privileges"]
            resp["all_privileges"].update(grant["privileges"])
        elif is_role_in_hierarchy_py(snowpark_session, grant["role_name"], role_name_py, []):
            resp["inherited_privileges"].append(grant)
            resp["all_privileges"].update(grant["privileges"])
    resp["all_privileges"] = list(resp["all_privileges"])
    return resp
'
call get_role_privileges_on_object('%(str1)s', '%(str2)s', '%(str3)s', '%(str4)s', '%(str5)s');
        """ % {"str1": self.role_name, "str2": self.db_name, "str3": self.schema_name, "str4": self.object_type, "str5": self.object_name}

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        return Config(cast=[ObjectType])
