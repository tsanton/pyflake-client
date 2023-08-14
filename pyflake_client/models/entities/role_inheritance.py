# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity
from pyflake_client.models.enums.privilege import Privilege

@dataclass(frozen=True)
class RoleInheritance(ISnowflakeEntity):
    """RoleInheritance"""

    principal_identifier: str
    principal_type: str
    inherited_role_identifier: str
    inherited_role_type: str
    privilege: Privilege
    grant_option: bool
    granted_by: str
    created_on: datetime