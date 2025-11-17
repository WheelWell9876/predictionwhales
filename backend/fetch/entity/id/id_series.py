"""
ID series
Handles individual fetching for the series
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

import requests
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class IdSeriesManager(DatabaseManager):
    """Manager for series-related operations with multithreading support"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0

    def fetch_series_by_id(self, series_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific series
        """
        try:
            url = f"{self.base_url}/series/{series_id}"
            params = {"include_chat": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            series = response.json()

            # Store detailed series
            self._store_series_detailed(series)

            # Process events in series
            if 'events' in series:
                self._store_series_events(series_id, series['events'])

            # Process collections in series
            if 'collections' in series:
                self._store_series_collections(series_id, series['collections'])

            return series

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching series {series_id}: {e}")
            return None

    def fetch_series_by_id_parallel(self, series_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific series with parallel sub-requests
        Processes events and collections in parallel
        """
        try:
            url = f"{self.base_url}/series/{series_id}"
            params = {"include_chat": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            series = response.json()

            # Parallel execution of sub-tasks
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []

                # Store series details
                futures.append(executor.submit(self._store_series_detailed, series))

                # Process events if present
                if 'events' in series:
                    futures.append(executor.submit(
                        self._store_series_events,
                        series_id,
                        series['events']
                    ))

                # Process collections if present
                if 'collections' in series:
                    futures.append(executor.submit(
                        self._store_series_collections,
                        series_id,
                        series['collections']
                    ))

                # Wait for all to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error in parallel series fetch subtask: {e}")

            return series

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching series {series_id}: {e}")
            return None