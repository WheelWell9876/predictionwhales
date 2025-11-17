"""
Markets Manager for Polymarket Terminal
Handles fetching, processing, and storing market data with multithreading support
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config

class MarketsManager(DatabaseManager):
    """Manager for market-related operations with multithreading support"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
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
    

    
    def _fetch_and_store_event_markets(self, event: Dict, idx: int, total_events: int) -> List[Dict]:
        """
        Helper method to fetch and store markets for a single event
        Thread-safe wrapper for parallel execution
        """
        event_id = event.get('id') if isinstance(event, dict) else event
        
        try:
            # Fetch markets for this event
            markets = self._fetch_markets_for_event(event_id)
            
            if markets:
                # Store markets (thread-safe)
                with self._db_lock:
                    self._store_markets(markets, event_id)
            
            # Update progress (thread-safe)
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0 or self._progress_counter == total_events:
                    self.logger.info(f"Progress: {self._progress_counter}/{total_events} events processed")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
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
            # First try to get markets from event endpoint
            url = f"{self.base_url}/events/{event_id}"
            params = {"include_markets": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                event_data = response.json()
                return event_data.get('markets', [])
            
            # Fallback to markets endpoint with event filter
            url = f"{self.base_url}/markets"
            params = {"event_id": event_id, "limit": self.config.MAX_MARKETS_PER_EVENT}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching markets for event {event_id}: {e}")
            return []
    

    

    

    
    def process_all_markets_detailed(self, use_parallel: bool = True):
        """
        Process all markets to fetch detailed information
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests (tags, OI)
        """
        # Get all market IDs from database
        markets = self.fetch_all("SELECT id, condition_id FROM markets ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(markets)} markets for detailed information using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Choose which fetch method to use
            fetch_method = self.fetch_market_by_id_parallel if use_parallel else self.fetch_market_by_id
            
            # Submit all tasks
            future_to_market = {
                executor.submit(self._process_market_detailed, market, fetch_method, len(markets)): market 
                for market in markets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing market {market['id']}: {e}")
        
        self.logger.info(f"Market processing complete. Processed: {self._progress_counter}, Errors: {self._error_counter}")
    
    def _process_market_detailed(self, market: Dict, fetch_method, total_markets: int):
        """
        Helper method to process a single market
        Thread-safe wrapper for parallel execution
        """
        try:
            fetch_method(market['id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 50 == 0:
                    self.logger.info(f"Processed {self._progress_counter}/{total_markets} markets")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e
    
    def batch_fetch_markets(self, market_ids: List[str], use_parallel: bool = True) -> List[Dict]:
        """
        Fetch multiple markets in parallel
        
        Args:
            market_ids: List of market IDs to fetch
            use_parallel: If True, uses parallel fetching for sub-requests
            
        Returns:
            List of market dictionaries
        """
        self.logger.info(f"Batch fetching {len(market_ids)} markets using {self.max_workers} threads...")
        
        markets = []
        fetch_method = self.fetch_market_by_id_parallel if use_parallel else self.fetch_market_by_id
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(fetch_method, market_id): market_id 
                for market_id in market_ids
            }
            
            for future in as_completed(future_to_id):
                market_id = future_to_id[future]
                try:
                    market = future.result()
                    if market:
                        markets.append(market)
                except Exception as e:
                    self.logger.error(f"Error fetching market {market_id}: {e}")
        
        return markets
    
    def fetch_global_open_interest(self) -> Optional[float]:
        """
        Fetch global open interest across all markets
        """
        try:
            url = f"{self.data_api_url}/oi"
            params = {"market": "GLOBAL"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                global_oi = data[0].get('value', 0)
                
                # Store global OI
                record = {
                    'market_id': 'GLOBAL',
                    'condition_id': 'GLOBAL',
                    'open_interest': global_oi,
                    'timestamp': datetime.now().isoformat()
                }
                self.insert_or_replace('market_open_interest', record)
                
                self.logger.info(f"Global Open Interest: ${global_oi:,.2f}")
                return global_oi
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching global open interest: {e}")
            return None
    
    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for market updates
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        self.logger.info("Starting daily market scan with multithreading...")
        
        # Get all active events
        events = self.fetch_all("SELECT id FROM events WHERE active = 1")
        
        # Fetch markets for active events (parallelized)
        all_markets = self.fetch_all_markets_from_events(events)
        
        # Process detailed information for markets (parallelized)
        self.process_all_markets_detailed(use_parallel=use_parallel)
        
        # Fetch global open interest
        if self.config.FETCH_OPEN_INTEREST:
            self.fetch_global_open_interest()
        
        self.logger.info(f"Daily market scan complete. Total markets: {len(all_markets)}")
        
        return len(all_markets)