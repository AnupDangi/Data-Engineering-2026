"""
Bronze Data Lake Ingestion Pipeline
-----------------------------------
Continuously streams CSV data into bronze storage at 1000 events/sec.

Architecture:
- CSV → Rate-limited stream (1000 events/sec) → Bronze storage
- Infinite loop: When CSV ends, restart immediately
- At-least-once delivery with idempotency
- Partitioned by ingestion time: year/month/day/hour
- File size: 128MB max (auto-flush)

Production Ready:
- Graceful shutdown on Ctrl+C
- Comprehensive logging
- Metrics tracking
"""

from csv_reader import stream_csv
from rate_controller import rate_limit
from event_builder import build_event
from bronze_writer import BronzeWriter
import logging
import sys

# Configure logging for production visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('producer_pipeline.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
CSV_PATH = "../data/ZomatoDataset.csv"           # Source data file
BATCH_SIZE = 1000                                # Events per second
INTERVAL_SECONDS = 1                             # Interval between batches
BRONZE_BASE_PATH = "../data/data_storage/bronze"  # Bronze layer destination (mounted to Airflow at /opt/airflow/data_lake)
MAX_FILE_SIZE_MB = 128                           # File size before flush

# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    """
    Main ingestion loop:
    1. Initialize bronze writer
    2. Stream CSV with rate limiting
    3. Write events to bronze storage
    4. Loop infinitely (restart CSV on completion)
    """
    
    logger.info("=" * 80)
    logger.info("🚀 Bronze Data Ingestion Pipeline Started")
    logger.info("=" * 80)
    logger.info(f"Source: {CSV_PATH}")
    logger.info(f"Destination: {BRONZE_BASE_PATH}")
    logger.info(f"Throughput: {BATCH_SIZE} events/sec")
    logger.info(f"File size: {MAX_FILE_SIZE_MB}MB")
    logger.info("=" * 80)
    
    # Initialize bronze writer with configured size limit
    writer = BronzeWriter(
        base_path=BRONZE_BASE_PATH, 
        max_file_size_mb=MAX_FILE_SIZE_MB
    )
    
    cycle_count = 0
    
    try:
        # Infinite loop: production systems run continuously
        while True:
            cycle_count += 1
            logger.info(f"📖 Starting CSV read cycle #{cycle_count}")
            
            # Stream CSV rows
            csv_stream = stream_csv(CSV_PATH)
            
            # Apply rate limiting: 1000 events/sec
            controlled_stream = rate_limit(
                csv_stream,
                batch_size=BATCH_SIZE,
                interval_seconds=INTERVAL_SECONDS
            )
            
            batch_count = 0
            
            # Process batches and write to bronze
            for batch in controlled_stream:
                batch_count += 1
                
                # Transform each row into event and write
                for row in batch:
                    event = build_event(row)
                    writer.add_event(event)
                
                # Log progress every 10 batches (10,000 events)
                if batch_count % 10 == 0:
                    stats = writer.get_stats()
                    buffer_mb = stats['buffer_size_bytes'] / (1024 * 1024)
                    logger.info(
                        f"📊 Cycle #{cycle_count} | Batch {batch_count} | "
                        f"Events: {stats['total_events_written']:,} | "
                        f"Files: {stats['total_files_written']} | "
                        f"Buffer: {buffer_mb:.2f}MB"
                    )
            
            # Flush any remaining buffered events at end of cycle
            writer.flush()
            
            logger.info(f"✅ Completed cycle #{cycle_count} | Restarting immediately...")
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Shutdown signal received (Ctrl+C)")
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        
    finally:
        # Graceful shutdown: flush buffer and log stats
        writer.close()
        logger.info("🏁 Pipeline stopped gracefully")
        logger.info("=" * 80)


if __name__ == "__main__":
    main()
