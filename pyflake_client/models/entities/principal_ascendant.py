# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Union

import dacite


@dataclass(frozen=True)
class PrincipalAscendant:
    """PrincipalAscendant"""

    grantee_identifier: str
    principal_type: str  # TODO: enum
    granted_identifier: str
    granted_on: str
    granted_by: str
    created_on: str  # TODO: datetime, not string. It's pyflake_client.describe() with json.loads() that fails this
    distance_from_source: int

    @staticmethod
    def map_key_names() -> Dict[str, str]:
        return {
            "grantee_name": "grantee_identifier",
            "granted_to": "principal_type",
            "role": "granted_identifier",
        }

    @classmethod
    def deserialize(cls, data: Dict[str, Any], config: Union[dacite.Config, None]) -> PrincipalAscendant:
        for old_key, new_key in cls.map_key_names().items():
            data[new_key] = data.pop(old_key)
        return PrincipalAscendant(**{k: data[k] for k in cls.__dataclass_fields__})
