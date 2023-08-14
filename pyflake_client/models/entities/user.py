# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime
from typing import Union

from pyflake_client.models.entities.snowflake_entity_interface import ISnowflakeEntity


@dataclass(frozen=True)
class User(ISnowflakeEntity):
    """User"""

    name: str
    created_on: Union[datetime, None] = None
    login_name: Union[str, None] = None
    display_name: Union[str, None] = None
    first_name: Union[str, None] = None
    last_name: Union[str, None] = None
    email: Union[str, None] = None
    mins_to_unlock: Union[str, None] = None
    days_to_expiry: Union[str, None] = None
    comment: Union[str, None] = None
    disabled: Union[str, None] = None
    default_warehouse: Union[str, None] = None
    default_namespace: Union[str, None] = None
    default_role: Union[str, None] = None
    deafult_secondary_roles: Union[str, None] = None
    ext_authn_duo: Union[str, None] = None
    ext_authn_uid: Union[str, None] = None
    mins_to_bypass_mfa: Union[str, None] = None
    owner: Union[str, None] = None
    last_successful_login: Union[datetime, None] = None
    expires_at_time: Union[datetime, None] = None
    locked_until_time: Union[datetime, None] = None
    has_password: Union[str, None] = None
    has_rsa_public_key: Union[str, None] = None
