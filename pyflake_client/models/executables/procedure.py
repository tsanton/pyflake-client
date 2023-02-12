"""procedure"""


from dataclasses import dataclass
from typing import List, Any
# from pyflake_client.models.enums.object_type import ObjectType

from pyflake_client.models.executables.snowflake_executable_interface import ISnowflakeExecutable


# TODO: ProcedureArg in Procedure.procedure_args -> must ensure order
# @dataclass(frozen=True)
# class ProcedureArg:
#     name: str
#     type: ObjectType
#     value: Any


@dataclass(frozen=True)
class Procedure(ISnowflakeExecutable):
    """Procedure"""
    database_name: str
    schema_name: str
    procedure_name: str
    procedure_args: List[Any]

    def get_exec_statement(self) -> str:
        # TODO: works only with strings for now -> implement procedure_args: List[ProcedureArg]
        params = [f"'{x}'" for x in self.procedure_args]
        return f"call {self.database_name}.{self.schema_name}.{self.procedure_name}({','.join(params)});"
