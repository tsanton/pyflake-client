from enum import Enum


class WarehouseSize(Enum):
    """
    enum for allowed warehouse sizes
    https://docs.snowflake.com/en/user-guide/warehouses-overview 
    """
    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"
    XXLARGE = "XXLARGE"
    XXXLARGE = "XXXLARGE"
    X4LARGE = "X4LARGE"
    # --- Before provisioning a 5X-Large or 6X-Large warehouse, please contact Snowflake Support. ---
    # X5LARGE = "X5LARGE"
    # X6LARGE= "X6LARG"
