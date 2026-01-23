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
    event_time = parse_event_time(
        row["Order_Date"],
        row["Time_Orderd"]
    )

    return {
        "event_id": row["ID"],
        "event_time": event_time,
        "ingestion_time": datetime.utcnow().isoformat(),
        "payload": row
    }
