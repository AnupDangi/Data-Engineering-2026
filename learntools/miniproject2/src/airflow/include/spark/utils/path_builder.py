"""
Path Builder Utilities
---------------------
Construct partition paths for data lake layers.
"""

from typing import Dict


def build_partition_path(year: int, month: int, day: int, hour: int) -> str:
    """
    Build Hive-style partition path.
    
    Args:
        year: Year (e.g., 2026)
        month: Month (1-12)
        day: Day (1-31)
        hour: Hour (0-23)
    
    Returns:
        Partition path string: "year=YYYY/month=MM/day=DD/hour=HH"
    
    Example:
        >>> build_partition_path(2026, 1, 24, 14)
        'year=2026/month=01/day=24/hour=14'
    """
    return f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}"


def build_bronze_path(base_path: str, year: int, month: int, day: int, hour: int) -> str:
    """Build full bronze layer path."""
    partition = build_partition_path(year, month, day, hour)
    return f"{base_path}/bronze/{partition}"


def build_silver_path(base_path: str, year: int, month: int, day: int, hour: int) -> str:
    """Build full silver layer path."""
    partition = build_partition_path(year, month, day, hour)
    return f"{base_path}/silver/{partition}"


def build_gold_path(base_path: str, metric_name: str = None) -> str:
    """
    Build full gold layer path.
    
    Gold layer is typically aggregated and not time-partitioned by hour.
    Instead partitioned by metric or business domain.
    """
    if metric_name:
        return f"{base_path}/gold/{metric_name}"
    return f"{base_path}/gold"


def parse_partition_from_path(path: str) -> Dict[str, int]:
    """
    Extract partition values from path.
    
    Args:
        path: Path containing Hive-style partitions
    
    Returns:
        Dict with year, month, day, hour
    
    Example:
        >>> parse_partition_from_path("bronze/year=2026/month=01/day=24/hour=14")
        {'year': 2026, 'month': 1, 'day': 24, 'hour': 14}
    """
    parts = path.split('/')
    result = {}
    
    for part in parts:
        if '=' in part:
            key, value = part.split('=')
            result[key] = int(value)
    
    return result
