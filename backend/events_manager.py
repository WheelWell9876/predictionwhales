"""
Events Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing event data with concurrent requests
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config

class EventsManager(DatabaseManager):
    """Manager for event-related operations with multithreading support"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        self._lock = Lock()  # Thread-safe database operations

    def process_all_events_detailed(self, num_threads: int = 10):
        """
        Process all events to fetch detailed information with multithreading
        
        Args:
            num_threads: Number of concurrent threads (default: 10)
        """
        # Get all event IDs from database
        events = self.fetch_all("SELECT id, slug FROM events ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(events)} events for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        lock = Lock()
        
        def process_event(event):
            nonlocal processed, errors
            try:
                self.fetch_event_by_id(event['id'])
                with lock:
                    processed += 1
                    if processed % 50 == 0:
                        self.logger.info(f"Processed {processed}/{len(events)} events")
            except Exception as e:
                with lock:
                    errors += 1
                self.logger.error(f"Error processing event {event['id']}: {e}")
        
        # Process events concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_event, events)
        
        self.logger.info(f"✅ Event processing complete. Processed: {processed}, Errors: {errors}")
    
    def daily_scan(self):
        """
        Perform daily scan for new events
        ONLY fetches active events
        """
        self.logger.info("Starting daily event scan...")
        
        # Fetch ONLY active events with multithreading
        active_events = self.fetch_all_events(closed=False, num_threads=5)
        
        # Process detailed information for new events with multithreading
        self.process_all_events_detailed(num_threads=10)
        
        self.logger.info("Daily event scan complete")
        
        return len(active_events)
    
    def remove_closed_events(self):
        """
        Remove all closed events from the database
        Use this to clean up if closed events were accidentally fetched
        """
        self.logger.info("Removing closed events from database...")
        
        # Count closed events
        result = self.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
        closed_count = result['count'] if result else 0
        
        self.logger.info(f"Found {closed_count} closed events to remove")
        
        if closed_count > 0:
            # Delete closed events
            deleted = self.delete_records('events', 'closed = 1', commit=True)
            self.logger.info(f"Removed {deleted} closed events")
            
            # Count remaining
            result = self.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 0")
            remaining = result['count'] if result else 0
            self.logger.info(f"Remaining active events: {remaining}")
            
            return deleted
        else:
            self.logger.info("No closed events to remove")
            return 0