"""
ID markets
Handles individual fetching for the markets
"""

from typing import Optional, Dict
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class IdMarketsManager(DatabaseManager):
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

    def fetch_market_by_id(self, market_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific market
        """
        try:
            url = f"{self.base_url}/markets/{market_id}"
            params = {"include_tag": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            market = response.json()

            # Store the detailed market
            self._store_market_detailed(market)

            # Fetch and store tags
            if 'tags' in market:
                self._store_market_tags(market_id, market['tags'])

            # Fetch open interest if enabled
            if self.config.FETCH_OPEN_INTEREST:
                self.fetch_market_open_interest(market_id, market.get('conditionId'))

            return market

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching market {market_id}: {e}")
            return None

    def fetch_market_by_id_parallel(self, market_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific market with parallel sub-requests
        Fetches market details, tags, and open interest concurrently
        """
        try:
            # Main market fetch
            url = f"{self.base_url}/markets/{market_id}"
            params = {"include_tag": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            market = response.json()

            # Parallel execution of sub-tasks
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []

                # Store market details
                futures.append(executor.submit(self._store_market_detailed, market))

                # Fetch tags if present
                if 'tags' in market:
                    futures.append(executor.submit(self._store_market_tags, market_id, market['tags']))

                # Fetch open interest if enabled
                if self.config.FETCH_OPEN_INTEREST and market.get('conditionId'):
                    futures.append(executor.submit(
                        self.fetch_market_open_interest,
                        market_id,
                        market.get('conditionId')
                    ))

                # Wait for all to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error in parallel market fetch subtask: {e}")

            return market

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching market {market_id}: {e}")
            return None




