from abc import ABC
from dataclasses import dataclass
from typing import Union


@dataclass
class DataType(ABC):
    nullable: bool
    length: Union[int, None]
    precision: Union[int, None]
    scale: Union[int, None]
    type: str


@dataclass
class ColumnType(ABC):
    name: str
    type: str
    nullable: bool


@dataclass
class Varchar(ColumnType):
    length: int


@dataclass
class Number(ColumnType):
    precision: int
    scale: int


@dataclass
class Bool(ColumnType):
    ...


@dataclass
class Column:
    name: str
    type: str
    data_type: DataType
