"""
Event Builder
------------
Transforms raw CSV rows into structured events with proper timestamps.
Handles various time format edge cases from real-world data.
"""

from datetime import datetime, timedelta
from typing import Dict


def parse_event_time(order_date: str, order_time: str) -> str:
    """
    Handles multiple real-world time formats:
    - HH:MM
    - 24:00 (rollover to next day)
    - Fraction of day (e.g. 0.458333333)
    - NaN or empty values (defaults to 00:00)
    Returns ISO formatted timestamp.
    """

    base_date = datetime.strptime(order_date, "%d-%m-%Y")

    # Handle NaN, empty, or None values
    if not order_time or order_time.strip().upper() in ("NAN", ""):
        order_time = "00:00"

    # Case 1: 24:00 → next day midnight
    if order_time == "24:00":
        event_dt = base_date + timedelta(days=1)
        event_dt = event_dt.replace(hour=0, minute=0)

    # Case 2: HH:MM
    elif ":" in order_time:
        event_dt = datetime.strptime(
            f"{order_date} {order_time}",
            "%d-%m-%Y %H:%M"
        )

    # Case 3: Fraction of day (Excel-style)
    else:
        try:
            fraction = float(order_time)
            total_minutes = int(round(fraction * 24 * 60))
            hours, minutes = divmod(total_minutes, 60)

            event_dt = base_date.replace(
                hour=hours % 24,
                minute=minutes
            )
        except ValueError:
            # Fallback to midnight for any unrecognized format
            event_dt = base_date.replace(hour=0, minute=0)

    return event_dt.isoformat()


def build_event(row: Dict[str, str]) -> Dict:
    """
    Build structured event from raw CSV row.
    
    Event Schema:
    - event_id: Unique identifier (used for idempotency)
    - event_time: When event occurred (parsed from order date/time)
    - ingestion_time: When event was ingested (UTC)
    - payload: Complete raw data for bronze layer
    
    Args:
        row: Dictionary from CSV row
        
    Returns:
        Dict: Structured event ready for bronze storage
    """
    ingestion_time = datetime.utcnow()
    event_time = parse_event_time(
        row["Order_Date"],
        row["Time_Orderd"]
    )

    # Generate unique event_id using CSV ID + ingestion timestamp to handle replays
    unique_event_id = f"{row['ID']}_{ingestion_time.strftime('%Y%m%d%H%M%S%f')}"

    return {
        "event_id": unique_event_id,
        "event_time": event_time,
        "ingestion_time": ingestion_time.isoformat(),
        "payload": row
    }
