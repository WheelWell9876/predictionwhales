"""
Batch markets
Handles batch fetching for the markets
"""

import requests
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_markets import StoreMarketsManager

class BatchMarketsManager(DatabaseManager):
    """Manager for batch market fetching with multithreading support"""
    
    def __init__(self):
        super().__init__()
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreMarketsManager()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Set max workers
        self.max_workers = min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))

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

    def _fetch_and_store_event_markets(self, event: Dict, idx: int, total: int) -> List[Dict]:
        """
        Fetch and store markets for a single event (thread-safe)
        """
        event_id = event.get('id')
        
        try:
            markets = self._fetch_markets_for_event(event_id)
            
            if markets:
                # Thread-safe storage
                with self._lock:
                    self.store_manager._store_markets(markets, event_id)
                
                with self._progress_lock:
                    self._progress_counter += 1
                    if self._progress_counter % 50 == 0 or self._progress_counter == total:
                        self.logger.info(f"  Progress: {self._progress_counter}/{total} events processed")
            
            return markets
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            self.logger.error(f"Error fetching markets for event {event_id}: {e}")
            return []

    def _fetch_markets_for_event(self, event_id: str) -> List[Dict]:
        """
        Fetch markets for a specific event
        """
        try:
            url = f"{self.base_url}/events/{event_id}/markets"
            params = {
                "limit": 100,
                "order": "volume",
                "ascending": "false"
            }
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            markets = response.json()
            return markets if markets else []
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching markets for event {event_id}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error for event {event_id}: {e}")
            return []