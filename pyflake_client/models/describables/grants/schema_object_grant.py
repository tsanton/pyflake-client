"""schema_object_grant"""
# pylint: disable=line-too-long
from dataclasses import dataclass

from dacite import Config

from pyflake_client.models.describables.snowflake_describable_interface import ISnowflakeDescribable
from pyflake_client.models.enums.object_type import ObjectType


@dataclass(frozen=True)
class SchemaObjectGrant(ISnowflakeDescribable):
    """SchemaObjectGrant"""
    db_name: str
    schema_name: str
    object_type: ObjectType
    object_name: str

    def get_describe_statement(self) -> str:
        """get_describe_statement"""
        return """
with get_privileges_on_object as procedure(db_name varchar, schema_name varchar, object_type varchar, object_name varchar)
  returns variant not null
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'get_privileges_on_object_py'
as '
def get_privileges_on_object_py(snowpark_session, db_name_py:str, schema_name_py:str, object_type_py:str, object_name_py: str):
    res = {}
    try:
        for row in snowpark_session.sql(f"SHOW GRANTS ON {object_type_py} {db_name_py}.{schema_name_py}.{object_name_py}").to_local_iterator():
            if row["granted_to"] == "ROLE":
                res.setdefault(row["grantee_name"], []).append(row["privilege"])
    except:
        pass
    return {
        "db_name": db_name_py,
        "schema_name": schema_name_py,
        "object_type": object_type_py,
        "object_name": object_name_py,
        "grants": [{"role_name": k, "privileges": v} for k, v in res.items()]
    }
'
call get_privileges_on_object('%(str1)s', '%(str2)s', '%(str3)s', '%(str4)s');
""" % {"str1": self.db_name, "str2": self.schema_name, "str3": self.object_type, "str4": self.object_name}

    def is_procedure(self) -> bool:
        """is_procedure"""
        return True

    def get_dacite_config(self) -> Config:
        return Config(cast=[ObjectType])
