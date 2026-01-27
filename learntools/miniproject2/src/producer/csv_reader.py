"""
CSV Streaming Reader
-------------------
Memory-efficient streaming reader for large CSV files.
Uses generator pattern to avoid loading entire file into memory.
"""

import csv
from typing import Iterator, Dict


def stream_csv(file_path: str) -> Iterator[Dict[str, str]]:
    """
    Stream CSV file row-by-row as dictionaries.
    
    This simulates a real-time data source where records arrive continuously
    instead of being loaded all at once. Critical for handling large datasets
    that don't fit in memory.
    
    Args:
        file_path: Path to the CSV file
        
    Yields:
        Dict[str, str]: Each row as a dictionary (column_name -> value)
    """
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


if __name__ == "__main__":
    # Test: stream without loading into memory
    file_path = "../data/ZomatoDataset.csv"
    count = 0
    for row in stream_csv(file_path):
        count += 1
        if count <= 3:
            print(f"Row {count}: {row}")
    print(f"Total rows: {count}")