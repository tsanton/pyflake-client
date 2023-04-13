from datetime import datetime

def parse_sf_datetime(input_str: str) -> datetime:
    try:
        return datetime.strptime(input_str, "%Y-%m-%d %H:%M:%S.%f%z")
    except ValueError as ex:
        raise ex 

def parse_sf_bool(input_str: str) -> bool:
    if input_str.lower() in ("true", "y", "yes"):
        return True
    elif input_str.lower() in ("false", "n", "no"):
        return False
    
    raise ValueError(f"Unrecognized bool string argument: {input_str}")