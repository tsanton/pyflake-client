# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class PrincipalAscendant(ISnowflakeEntity):
    grantee_identifier: str
    principal_type: str
    granted_identifier: str
    granted_on: str
    granted_by: str
    created_on: datetime
    distance_from_source: int
