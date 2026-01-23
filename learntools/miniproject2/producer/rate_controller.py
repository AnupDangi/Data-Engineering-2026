import time 
from typing import Iterable, List, Iterator


def rate_limit(
        iterator:Iterable,
        batch_size:int,
        interval_seconds:float
) -> Iterator[List]:
    """
        A generator that yields items from the input iterator in batches of a specified size,
        pausing for a specified interval between batches to control the rate of processing.

        Args:
            iterator (Iterable): An iterable source of items to be processed.
            batch_size (int): The number of items to include in each batch.
            interval_seconds (float): The time to wait between yielding batches, in seconds.
            yields: Lists of items, each of size batch_size (except possibly the last one).

        Eg: 100 items every 1 second
    """

    batch =[]

    for item in iterator: 
        batch.append(item)

        if len(batch)==batch_size:
            start_time=time.time()
            yield batch 

            elapsed=time.time()-start_time
            sleep_time=max(0,interval_seconds -elapsed)
            time.sleep(sleep_time)

            batch=[]
    
    ## Emit reamining items (if any)
    if batch: 
        yield batch 
        