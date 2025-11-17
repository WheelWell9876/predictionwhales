"""
Batch markets
Handles batch fetching for the markets
"""

from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class BatchMarketsManager(DatabaseManager):
    """Manager for market-related operations with multithreading support"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0

    def fetch_all_markets_from_events(self, events: List[Dict]) -> List[Dict]:
        """
        Fetch all markets from a list of events using multithreading
        Used after fetching events to get their markets
        """
        all_markets = []
        total_events = len(events)

        self.logger.info(f"Fetching markets for {total_events} events using {self.max_workers} threads...")

        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_event = {
                executor.submit(self._fetch_and_store_event_markets, event, idx, total_events): event
                for idx, event in enumerate(events, 1)
            }

            # Process completed tasks
            for future in as_completed(future_to_event):
                event = future_to_event[future]
                try:
                    markets = future.result()
                    if markets:
                        all_markets.extend(markets)
                except Exception as e:
                    event_id = event.get('id') if isinstance(event, dict) else event
                    self.logger.error(f"Error in thread processing event {event_id}: {e}")

        self.logger.info(f"Total markets fetched: {len(all_markets)}")
        self.logger.info(f"Errors encountered: {self._error_counter}")
        return all_markets