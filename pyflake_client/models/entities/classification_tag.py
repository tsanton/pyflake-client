"""table"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Union

import dacite


@dataclass(frozen=True)
class ClassificationTag:
    tag_database_name: str
    tag_schema_name: str
    tag_name: str
    domain_level: str
    tag_value: Union[str, None] = None

    @classmethod
    def load_from_sf(
        cls, data: Dict[str, Any], config: Union[dacite.Config, None] = None
    ) -> ClassificationTag:
        return ClassificationTag(
            tag_database_name=data["TAG_DATABASE"],
            tag_schema_name=data["TAG_SCHEMA"],
            tag_name=data["TAG_NAME"],
            domain_level=data["DOMAIN"],
            tag_value=data["TAG_VALUE"] if data["TAG_VALUE"] != "" else None,
        )
