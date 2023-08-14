# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.privilege import Privilege


@dataclass(frozen=True)
class RoleGrant(ISnowflakeEntity):
    """Grant"""

    grantee_identifier: str
    grantee_type: str
    granted_on: str
    granted_identifier: str
    privilege: Privilege
    grant_option: bool
    granted_by: str
    created_on: datetime
