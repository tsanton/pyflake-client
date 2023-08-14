# -*- coding: utf-8 -*-
from enum import Enum


class Privilege(str, Enum):
    """Privilege"""

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    TRUNCATE = "TRUNCATE"
    REFERENCES = "REFERENCES"
    REBUILD = "REBUILD"
    CREATE_SCHEMA = "CREATE SCHEMA"
    IMPORTED_PRIVILEGES = "IMPORTED PRIVILEGES"
    MODIFY = "MODIFY"
    OPERATE = "OPERATE"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    READ = "READ"
    REFERENCE_USAGE = "REFERENCE_USAGE"
    USAGE = "USAGE"
    WRITE = "WRITE"
    CREATE_TABLE = "CREATE TABLE"
    CREATE_TAG = "CREATE TAG"
    CREATE_VIEW = "CREATE VIEW"
    CREATE_FILE_FORMAT = "CREATE FILE FORMAT"
    CREATE_STAGE = "CREATE STAGE"
    CREATE_PIPE = "CREATE PIPE"
    CREATE_STREAM = "CREATE STREAM"
    CREATE_TASK = "CREATE TASK"
    CREATE_SEQUENCE = "CREATE SEQUENCE"
    CREATE_FUNCTION = "CREATE FUNCTION"
    CREATE_PROCEDURE = "CREATE PROCEDURE"
    CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE"
    CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW"
    CREATE_ROW_ACCESS_POLICY = "CREATE ROW ACCESS POLICY"
    CREATE_TEMPORARY_TABLE = "CREATE TEMPORARY TABLE"
    CREATE_MASKING_POLICY = "CREATE MASKING POLICY"
    CREATE_NETWORK_POLICY = "CREATE NETWORK POLICY"
    CREATE_DATA_EXCHANGE_LISTING = "CREATE DATA EXCHANGE LISTING"
    CREATE_ACCOUNT = "CREATE ACCOUNT"
    CREATE_SHARE = "CREATE SHARE"
    IMPORT_SHARE = "IMPORT SHARE"
    OVERRIDE_SHARE_RESTRICTIONS = "OVERRIDE SHARE RESTRICTIONS"
    ADD_SEARCH_OPTIMIZATION = "ADD SEARCH OPTIMIZATION"
    APPLY_MASKING_POLICY = "APPLY MASKING POLICY"
    APPLY_ROW_ACCESS_POLICY = "APPLY ROW ACCESS POLICY"
    APPLY_TAG = "APPLY TAG"
    APPLY = "APPLY"
    ATTACH_POLICY = "ATTACH POLICY"
    CREATE_ROLE = "CREATE ROLE"
    CREATE_USER = "CREATE USER"
    CREATE_WAREHOUSE = "CREATE WAREHOUSE"
    CREATE_DATABASE = "CREATE DATABASE"
    CREATE_DATABASE_ROLE = "CREATE DATABASE ROLE"
    CREATE_INTEGRATION = "CREATE INTEGRATION"
    MANAGE_GRANTS = "MANAGE GRANTS"
    MONITOR_USAGE = "MONITOR USAGE"
    MONITOR_EXECUTION = "MONITOR EXECUTION"
    EXECUTE_TASK = "EXECUTE TASK"
    EXECUTE_MANAGED_TASK = "EXECUTE MANAGED TASK"
    MANAGE_ORGANIZATION_SUPPORT_CASES = "MANAGE ORGANIZATION SUPPORT CASES"
    MANAGE_ACCOUNT_SUPPORT_CASES = "MANAGE ACCOUNT SUPPORT CASES"
    MANAGE_USER_SUPPORT_CASES = "MANAGE USER SUPPORT CASES"
