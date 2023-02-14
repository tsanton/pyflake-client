"""procedure"""
# pylint: disable=line-too-long
from dataclasses import dataclass
from typing import List
from pyflake_client.models.enums.column_type import ColumnType
from pyflake_client.models.executables.procedure_arg import ProcedureArg

from pyflake_client.models.executables.snowflake_executable_interface import ISnowflakeExecutable


@dataclass(frozen=True)
class Procedure(ISnowflakeExecutable):
    """Procedure"""
    database_name: str
    schema_name: str
    procedure_name: str
    procedure_args: List[ProcedureArg]

    def get_call_statement(self) -> str:
        proc_args = ""
        for arg in sorted(self.procedure_args, key=lambda x: x.positional_order):
            # Not using match self.type: for python 3.8 compatability
            if arg.data_type == ColumnType.VARCHAR:
                proc_args += f"'{str(arg.value)}',"
            elif arg.data_type == ColumnType.INTEGER:
                proc_args += f"{int(arg.value)},"
            else:
                raise ValueError(f"ColumnType ${arg.data_type} is not mapped")
        return f"call {self.database_name}.{self.schema_name}.{self.procedure_name}({proc_args.rstrip(',')});"
