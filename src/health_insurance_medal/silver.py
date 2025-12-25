import re
from typing import Optional


def to_snake_case(name: str) -> str:
    """
    Convert column names to snake_case and make them Delta-friendly.
    """
    if name is None:
        return None

    name = name.strip()
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def parse_holding_policy_duration(value: Optional[str]) -> Optional[int]:
    """
    Convert Holding_Policy_Duration to integer years.

    Examples:
    - "14+" -> 14
    - "1.0" -> 1
    - None  -> None
    """
    if value is None:
        return None

    value = value.strip()

    if value == "":
        return None

    if value == "14+":
        return 14

    try:
        return int(float(value))
    except ValueError:
        return None


def normalize_is_spouse(value: Optional[str]) -> Optional[bool]:
    """
    Normalize Is_Spouse field to boolean.
    """
    if value is None:
        return None

    value = value.strip().lower()

    if value == "yes":
        return True
    if value == "no":
        return False

    return None
