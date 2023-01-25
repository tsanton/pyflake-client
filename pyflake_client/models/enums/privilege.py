"""privilege_enum"""
from enum import Enum


class Privilege(str, Enum):
    """Privilege"""
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    REFERECES = "REFERECES"
