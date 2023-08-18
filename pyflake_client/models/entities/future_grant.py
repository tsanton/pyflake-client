# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass(frozen=True)
class FutureGrant(ISnowflakeEntity):
    grantee_identifier: str
    grantee_type: str
    grant_on: str
    grant_identifier: str
    privilege: Privilege
    grant_option: bool
    created_on: datetime
