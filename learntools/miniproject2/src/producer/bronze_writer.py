"""
Bronze Storage Writer
--------------------
Writes raw events to bronze layer with time-based partitioning.
Implements at-least-once delivery semantics with idempotency.

File Format: JSONL (newline-delimited JSON)
Partitioning: By ingestion time (year/month/day/hour)
File Size: Configurable max size (default 128MB)
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class BronzeWriter:
    
    """
    Buffered writer for bronze storage layer.
    
    Responsibilities:
    - Buffer events in memory until size threshold
    - Write to partitioned storage (year/month/day/hour)
    - Ensure idempotency within session
    - Provide flush on demand and auto-flush on size limit
    """
    
    def __init__(self, base_path: str = "./data/data_storage/bronze", max_file_size_mb: int = 128):
        """
        Initialize bronze writer.
        
        Args:
            base_path: Root directory for bronze storage
            max_file_size_mb: Max file size in MB before auto-flush
        """
        self.base_path = Path(base_path)
        self.max_file_size_bytes = max_file_size_mb * 1024 * 1024
        
        # In-memory buffer for batching writes
        self.buffer = []
        self.buffer_size = 0
        
        # Current file tracking - append to same file until 128MB reached
        self.current_file_path = None
        self.current_file_size = 0
        
        # Idempotency: track event_ids written in current session
        # In production, this would be in external store (Redis/DB)
        self.seen_event_ids = set()
        
        # Metrics
        self.total_events_written = 0
        self.total_files_written = 0
        self.duplicate_events_skipped = 0
        
        # Ensure base directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"🎯 BronzeWriter initialized: base_path={self.base_path}, max_file_size={max_file_size_mb}MB")
        logger.info(f"📝 Strategy: Append to single file until {max_file_size_mb}MB, then create new file")
    
    def add_event(self, event: Dict) -> None:
        """
        Add event to buffer with idempotency check.
        Auto-flushes when buffer exceeds max size.
        
        Args:
            event: Event dictionary with 'event_id' field required
        """
        event_id = event.get("event_id")
        
        # Idempotency: skip if already written in this session
        if event_id in self.seen_event_ids:
            self.duplicate_events_skipped += 1
            logger.debug(f"Duplicate event_id={event_id} skipped")
            return
        
        # Calculate event size (JSON serialized + newline)
        event_json = json.dumps(event)
        event_size = len(event_json.encode('utf-8')) + 1  # +1 for \n
        
        # Add to buffer
        self.buffer.append(event)
        self.buffer_size += event_size
        self.seen_event_ids.add(event_id)
        
        # Auto-flush if buffer exceeds threshold
        if self.buffer_size >= self.max_file_size_bytes:
            self.flush()
    
    def flush(self) -> None:
        """
        Write buffered events to disk and clear buffer.
        
        Strategy:
        - APPEND to current file if it exists and hasn't reached 128MB
        - CREATE new file only when current file >= 128MB or no current file
        - Partition by ingestion time (UTC): year/month/day/hour
        """
        if not self.buffer:
            logger.debug("Flush called but buffer is empty, skipping")
            return
        
        # Check if we need a new file (current file >= 128MB or no current file)
        need_new_file = (
            self.current_file_path is None or 
            self.current_file_size >= self.max_file_size_bytes
        )
        
        if need_new_file:
            # Partition by ingestion time (UTC)
            now = datetime.utcnow()
            partition_path = (
                self.base_path 
                / f"year={now.year}" 
                / f"month={now.month:02d}" 
                / f"day={now.day:02d}" 
                / f"hour={now.hour:02d}"
            )
            partition_path.mkdir(parents=True, exist_ok=True)
            
            # Generate unique filename with microsecond precision
            timestamp_str = now.strftime("%Y%m%d%H%M%S%f")
            self.current_file_path = partition_path / f"events_{timestamp_str}.json"
            self.current_file_size = 0
            
            logger.info(f"📄 Creating NEW file: {self.current_file_path.relative_to(self.base_path)}")
        
        # Append to current file (mode='a' for append)
        with open(self.current_file_path, 'a', encoding='utf-8') as f:
            for event in self.buffer:
                f.write(json.dumps(event) + '\n')
        
        # Update current file size
        self.current_file_size += self.buffer_size
        
        # Calculate sizes for logging
        buffer_size_mb = self.buffer_size / (1024 * 1024)
        current_file_mb = self.current_file_size / (1024 * 1024)
        
        # Log write confirmation with detailed size info
        logger.info(
            f"✅ Appended {len(self.buffer):,} events ({buffer_size_mb:.2f}MB) → "
            f"{self.current_file_path.name} | "
            f"Total file size: {current_file_mb:.2f}MB / {self.max_file_size_bytes/(1024*1024):.0f}MB"
        )
        
        # Update metrics
        self.total_events_written += len(self.buffer)
        if need_new_file:
            self.total_files_written += 1
        
        # Clear buffer (at-least-once: buffer cleared AFTER successful write)
        self.buffer = []
        self.buffer_size = 0
        
        # Clear idempotency set after each file to prevent memory leak
        # This allows CSV replays to create new files with unique event_ids
        self.seen_event_ids.clear()
    
    def get_stats(self) -> Dict:
        """Return current writer statistics including buffered events."""
        return {
            "total_events_written": self.total_events_written + len(self.buffer),  # Include buffer
            "total_files_written": self.total_files_written,
            "duplicate_events_skipped": self.duplicate_events_skipped,
            "buffer_size_bytes": self.buffer_size,
            "buffer_events": len(self.buffer)
        }
    
    def close(self) -> None:
        """
        Flush remaining buffer and log final statistics.
        Called on graceful shutdown.
        """
        self.flush()
        stats = self.get_stats()
        logger.info(
            f"BronzeWriter closed | "
            f"Events: {stats['total_events_written']} | "
            f"Files: {stats['total_files_written']} | "
            f"Duplicates skipped: {stats['duplicate_events_skipped']}"
        )
