# -*- coding: utf-8 -*-
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Union

import dacite

from pyflake_client.models.describables.snowflake_describable_interface import (
    ISnowflakeDescribable,
)
from pyflake_client.models.entities.tag import Tag as TagEntity


@dataclass(frozen=True)
class Tag(ISnowflakeDescribable):
    database_name: Union[str, None]
    schema_name: Union[str, None]
    tag_name: Union[str, None]

    def get_describe_statement(self) -> str:
        query = f"SHOW TAGS LIKE '{self.tag_name}' IN SCHEMA {self.database_name}.{self.schema_name}".upper()
        return query

    def is_procedure(self) -> bool:
        return False

    @classmethod
    def get_deserializer(cls) -> Callable[[Dict[str, Any]], TagEntity]:
        def deserialize(data: Dict[str, Any]) -> TagEntity:
            return dacite.from_dict(
                TagEntity,
                data,
                dacite.Config(
                    type_hooks={
                        # datetime: lambda v: datetime.fromisoformat(v),
                        List[str]: lambda v: json.loads(v),
                    }
                ),
            )

        return deserialize
