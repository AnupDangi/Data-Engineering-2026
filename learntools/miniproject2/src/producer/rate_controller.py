"""
Rate Controller
--------------
Controls throughput of event streams to prevent overwhelming downstream systems.
Implements precise rate limiting with batch emission.
"""

import time
from typing import Iterable, List, Iterator


def rate_limit(
    iterator: Iterable,
    batch_size: int,
    interval_seconds: float
) -> Iterator[List]:
    """
    Yield items in controlled batches with timed intervals.
    
    Critical for production systems to:
    - Prevent overwhelming downstream consumers
    - Maintain predictable throughput
    - Enable backpressure handling
    
    Args:
        iterator: Source stream of items (e.g., CSV rows)
        batch_size: Number of items per batch (e.g., 1000 = 1000 events/sec if interval=1)
        interval_seconds: Time to wait between batches
        
    Yields:
        List: Batches of items (size = batch_size, except last batch)
        
    Example:
        >>> stream = rate_limit(data_source, batch_size=1000, interval_seconds=1)
        >>> # Yields 1000 items per second
        
    Implementation:
        - Accumulates items into batch
        - Yields when batch is full
        - Sleeps to maintain target interval
        - Accounts for processing time in sleep calculation
    """
    batch = []

    for item in iterator:
        batch.append(item)

        if len(batch) == batch_size:
            start_time = time.time()
            yield batch

            # Sleep for remaining interval (accounting for processing time)
            elapsed = time.time() - start_time
            sleep_time = max(0, interval_seconds - elapsed)
            time.sleep(sleep_time)

            batch = []
    
    # Emit remaining items (final partial batch)
    if batch:
        yield batch 
        