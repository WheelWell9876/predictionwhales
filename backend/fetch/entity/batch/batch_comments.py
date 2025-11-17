"""
Batch comments
Handles batch fetching for the comments of markets events
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class BatchCommentsManager(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0


    def fetch_comments_for_all_events(self, limit_per_event: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active events with multithreading
        
        Args:
            limit_per_event: Number of comments to fetch per event (default: 15)
            
        Returns:
            Dictionary with statistics
        """
        self.logger.info(f"💬 Fetching top {limit_per_event} comments for all active events...")
        
        # Get all active events
        events = self.fetch_all("""
            SELECT id, title FROM events 
            WHERE active = 1 
            ORDER BY volume DESC
        """)
        
        self.logger.info(f"Processing {len(events)} events using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_event = {
                executor.submit(self._fetch_and_store_event_comments, event, limit_per_event, len(events)): event 
                for event in events
            }
            
            # Process completed tasks
            for future in as_completed(future_to_event):
                event = future_to_event[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing event {event['id']}: {e}")
        
        self.logger.info(f"✅ Comments fetch complete!")
        self.logger.info(f"   Events processed: {self._progress_counter}")
        self.logger.info(f"   Comments fetched: {self._comments_counter}")
        self.logger.info(f"   Reactions fetched: {self._reactions_counter}")
        self.logger.info(f"   Errors: {self._error_counter}")
        
        return {
            'events_processed': self._progress_counter,
            'comments_fetched': self._comments_counter,
            'reactions_fetched': self._reactions_counter,
            'errors': self._error_counter
        }

    def fetch_comments_for_all_markets(self, limit_per_market: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active markets with multithreading
        
        Args:
            limit_per_market: Number of comments to fetch per market (default: 15)
            
        Returns:
            Dictionary with statistics
        """
        self.logger.info(f"💬 Fetching top {limit_per_market} comments for all active markets...")
        
        # Get all active markets
        markets = self.fetch_all("""
            SELECT id, question FROM markets 
            WHERE active = 1 
            ORDER BY volume DESC
        """)
        
        self.logger.info(f"Processing {len(markets)} markets using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_market = {
                executor.submit(self._fetch_and_store_market_comments, market, limit_per_market, len(markets)): market 
                for market in markets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing market {market['id']}: {e}")
        
        self.logger.info(f"✅ Comments fetch complete!")
        self.logger.info(f"   Markets processed: {self._progress_counter}")
        self.logger.info(f"   Comments fetched: {self._comments_counter}")
        self.logger.info(f"   Reactions fetched: {self._reactions_counter}")
        self.logger.info(f"   Errors: {self._error_counter}")
        
        return {
            'markets_processed': self._progress_counter,
            'comments_fetched': self._comments_counter,
            'reactions_fetched': self._reactions_counter,
            'errors': self._error_counter
        }

