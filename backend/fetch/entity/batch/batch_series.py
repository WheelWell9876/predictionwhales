"""
Batch series
Handles batch fetching for the series
"""

import time
import requests
from typing import Dict, List
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_series import StoreSeriesManager

class BatchSeriesManager(DatabaseManager):
    """Manager for batch series fetching"""
    
    def __init__(self):
        super().__init__()
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreSeriesManager()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Set max workers
        self.max_workers = min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))

    def fetch_all_series(self, limit: int = 100) -> List[Dict]:
        """
        Fetch all series from the API
        Used for initial data load and daily scans
        """
        all_series = []
        offset = 0

        self.logger.info("Starting to fetch all series...")

        while True:
            try:
                url = f"{self.base_url}/series"
                params = {
                    "limit": limit,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "false",
                    "include_chat": "true"
                }

                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                series_list = response.json()

                if not series_list:
                    break

                all_series.extend(series_list)

                # Store series
                self.store_manager._store_series_list(series_list)

                self.logger.info(f"Fetched {len(series_list)} series (offset: {offset})")

                offset += limit

                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)

                if len(series_list) < limit:
                    break

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching series at offset {offset}: {e}")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                break

        self.logger.info(f"Total series fetched: {len(all_series)}")
        return all_series