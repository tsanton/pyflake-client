
from abc import abstractmethod, ABC
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Union
import re

@dataclass
class ClassificationTag:
    database_name: str
    schema_name: str
    tag_name: str
    tag_value: Union[str, None] = None

    def get_identifier(self) -> str:
        return f"{self.database_name}.{self.schema_name}.{self.tag_name}"


@dataclass
class Identity:
    start_number: int = 1
    increment_number: int = 1

    def to_string(self):
        return f"IDENTITY START {self.start_number} INCREMENT {self.increment_number}"


@dataclass
class ForeignKey:
    database_name: str
    schema_name: str
    table_name: str
    column_name: str


@dataclass
class Sequence:
    # what this? # TODO
    ...


@dataclass
class Column(ABC):
    """base class for all snowflake column classes
    if the inheriting class has the @dataclass decorator,
    it must also provide a default value for every field"""
    name: str
    primary_key: bool = False
    not_null: bool = False
    unique: bool = False
    tags: List[ClassificationTag] = field(default_factory=list)
    foreign_key: Union[ForeignKey, None] = None

    def __post_init__(self):
        """Must contain only letters (a-z, A-Z), numbers (0-9), or underscores ( _ )
        Must begin with a letter or underscore.
        Must be less than the maximum length of 251 characters"""
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,250}$", self.name) is None:
            raise ValueError("Invalid name, column name must match ^[a-zA-Z_][a-zA-Z0-9_]{0,250}$")
        
        # TODO : how can we check if the foreign_key is valid? probably not here ...
        self.check_constraints()


    @abstractmethod
    def check_constraints(self):
        ...

    @abstractmethod
    def get_definition(self) -> str:
        ...


@dataclass
class Varchar(Column):
    length: int = 16777216
    collation: Union[str, None] = None
    default_value: Union[str, None] = None

    def check_constraints(self):
        if self.length < 0 or self.length > 16777216:
            raise ValueError("Varchar length must be >= 0 and <= 16777216")

    def get_definition(self) -> str:
        definition = f"{self.name} VARCHAR ({self.length})"
        if self.not_null:
            definition += " NOT NULL"
        if self.unique:
            definition += " UNIQUE"
        if self.default_value is not None:
            definition += f" DEFAULT {self.default_value}"
        if self.collation is not None:
            definition += f" COLLATE '{self.collation}'"
        if self.foreign_key is not None:
            raise NotImplementedError("Foreign Keys not supported as of now")

        return definition


@dataclass
class Number(Column):
    # Precision refers to the 'xxx' of xxx.yyy -> max 38
    precision: int = 38
    # Scale refers to `yyy` of xxx.yyy -> max 37
    scale: int = 37
    default_value: Union[Decimal, None] = None
    identity: Union[Identity, None] = None
    sequence: Union[Sequence, None] = None

    def check_constraints(self):
        if self.precision > 38 or self.precision < 0:
            raise ValueError("Precision must be between 1 and 38")
        if self.scale > 37 or self.scale < 0:
            raise ValueError("Scale must be between 0 and 37")
        

    def get_definition(self) -> str:
        definition = f"{self.name} NUMBER({self.precision},{self.scale})"
        if self.not_null:
            definition += " NOT NULL"
        if self.unique:
            definition += " UNIQUE"
        if self.default_value is not None:
            definition += f" DEFAULT {self.default_value}"
        if self.identity is not None:
            definition += f" IDENTITY({self.identity.start_number},{self.identity.increment_number})"
        if self.sequence is not None:
            raise NotImplementedError("Sequences are not supported as of now")
        if self.foreign_key is not None:
            raise NotImplementedError("Foreign Keys not supported as of now")
        
        return definition
    
class Int:
    """template for how to implement the other numeric types
    NUMBER
    DECIMAL
    INT, INTEGER, BIGINT, SMALLINT
    are all of type NUMBER in snowflake"""
    def __new__(
            cls,
            name,
            primary_key=False,
            not_null=False,
            unique=False,
            tags=[],
            foreign_key=None,
            default_value=None,
            identity=None,
            sequence=None,
    ):
        return Number(
            name=name,
            primary_key=primary_key,
            not_null=not_null,
            unique=unique,
            tags=tags,
            foreign_key=foreign_key,
            default_value=default_value,
            precision=38,
            scale=0,
            identity=identity,
            sequence=sequence
        )
    
if __name__ == "__main__":
    vc = Varchar(
        name="my_new_varchar",
        primary_key=False,
        not_null=False,
        unique=True,
        tags=[],
        length=1
    )
    l: List[Column] = [vc]
    print(vc.get_definition())
    i = Int("my_int")
    print(i.get_definition())
    try:
        vc2 = Varchar(name="1_bla", primary_key=False, not_null=False, unique=True, tags=[], length=123)
    except ValueError as ex:
        print(ex)

    try:
        vc2 = Varchar(name="_bla", primary_key=False, not_null=False, unique=True, tags=[], length=9999999999999999999)
    except ValueError as ex:
        print(ex)

    print(vc.get_definition())
    n = Number(name="my_number", primary_key=False, not_null=True, unique=True, precision=25, scale=13, default_value=Decimal(1.00001))
    print(n.get_definition())

   