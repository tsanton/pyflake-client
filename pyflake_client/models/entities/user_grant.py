# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class UserGrant(ISnowflakeEntity):
    grantee_identifier: str
    grantee_type: str
    granted_identifier: str
    granted_by: str
    created_on: datetime
