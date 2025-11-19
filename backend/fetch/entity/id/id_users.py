"""
ID users
Handles individual fetching for the users
"""

from datetime import datetime
import time
from typing import Dict, List, Set, Optional
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_users import StoreUsersManager

class IdUsersManager(DatabaseManager):
    """Manager for individual user fetching"""

    def __init__(self):
        super().__init__()
        self.config = Config
        self.data_api_url = Config.DATA_API_URL
        self.base_url = Config.GAMMA_API_URL
        self.clob_url = Config.CLOB_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreUsersManager()
        
        # Whale thresholds
        self.MIN_WALLET_VALUE = 1000  # $1000 minimum wallet value
        self.MIN_POSITION_VALUE = 250  # $250 minimum position value
        self.TOP_HOLDERS_PER_MARKET = 25  # Top 25 holders per market
        self.MIN_TRANSACTION_SIZE = Config.MIN_TRANSACTION_SIZE if hasattr(Config, 'MIN_TRANSACTION_SIZE') else 500
        
        # Set max workers
        self.max_workers = min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe counters and collections
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._whale_wallets = set()

    def enrich_all_whale_users(self) -> Dict[str, int]:
        """Enrich all whale users with complete profile data using multithreading"""
        self.logger.info("🔍 Enriching whale user profiles with multithreading...")
        
        # Get all whale users
        whales = self.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        whale_wallets = [w['proxy_wallet'] for w in whales]
        
        self.logger.info(f"Enriching {len(whale_wallets)} whale users using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_wallet = {
                executor.submit(self._enrich_single_whale_thread_safe, wallet, len(whale_wallets)): wallet 
                for wallet in whale_wallets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_wallet):
                wallet = future_to_wallet[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error enriching whale {wallet}: {e}")
        
        self.logger.info(f"✅ Enriched {self._progress_counter} whale users, Errors: {self._error_counter}")
        
        return {
            'total_whales_enriched': self._progress_counter,
            'errors': self._error_counter
        }

    def batch_enrich_whales(self, wallet_addresses: List[str]) -> Dict[str, int]:
        """Enrich multiple whale users in parallel"""
        self.logger.info(f"Batch enriching {len(wallet_addresses)} whale users...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_wallet = {
                executor.submit(self._enrich_single_whale_thread_safe, wallet, len(wallet_addresses)): wallet 
                for wallet in wallet_addresses
            }
            
            for future in as_completed(future_to_wallet):
                wallet = future_to_wallet[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error enriching whale {wallet}: {e}")
        
        return {
            'total_enriched': self._progress_counter,
            'errors': self._error_counter
        }

    def _enrich_single_whale_thread_safe(self, proxy_wallet: str, total_whales: int):
        """Thread-safe wrapper for enriching a single whale user"""
        try:
            self._enrich_whale_user_data(proxy_wallet)
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0:
                    self.logger.info(f"  Enriched {self._progress_counter}/{total_whales} whale users")
            
            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

    def _enrich_whale_user_data(self, proxy_wallet: str):
        """Fetch and store complete data for a whale user"""
        # Import positions manager for fetching positions
        from backend.positions_manager import PositionsManager
        positions_mgr = PositionsManager()
        
        # Use ThreadPoolExecutor for parallel sub-requests
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            # Fetch all user data in parallel
            futures.append(executor.submit(self._fetch_user_trades, proxy_wallet))
            futures.append(executor.submit(self._fetch_user_activity, proxy_wallet))
            futures.append(executor.submit(positions_mgr.fetch_user_current_positions, proxy_wallet))
            futures.append(executor.submit(positions_mgr.fetch_user_closed_positions, proxy_wallet))
            
            # Wait for all to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error in user enrichment subtask: {e}")

    def _fetch_user_trades(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user trade history"""
        try:
            url = f"{self.data_api_url}/trades"
            params = {"user": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                trades = response.json() or []
                if trades:
                    with self._lock:
                        self.store_manager._store_user_trades(proxy_wallet, trades)
                return trades
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching trades for {proxy_wallet}: {e}")
            return []

    def _fetch_user_activity(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user activity history"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {"user": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                activity = response.json() or []
                if activity:
                    with self._lock:
                        self.store_manager._store_user_activity(proxy_wallet, activity)
                return activity
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching activity for {proxy_wallet}: {e}")
            return []





    def _fetch_user_wallet_value(self, proxy_wallet: str) -> float:
        """Fetch user's total wallet value"""
        try:
            url = f"{self.data_api_url}/portfolio-value"
            params = {"user": proxy_wallet}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('totalValue', 0) if data else 0
            
            return 0
            
        except Exception as e:
            return 0

    def _fetch_user_value_api(self, proxy_wallet: str) -> Dict:
        """Fetch portfolio value for a single user from API"""
        try:
            url = f"{self.data_api_url}/value"
            params = {"user": proxy_wallet}
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}
            
            value_data = response.json()
            
            if not value_data or len(value_data) == 0:
                return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}
            
            total_value = value_data[0].get('value', 0)
            
            return {'fetched': True, 'wallet': proxy_wallet, 'value': total_value}
            
        except Exception as e:
            self.logger.debug(f"Error fetching value for {proxy_wallet}: {e}")
            return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}

    def _fetch_user_activity_api(self, proxy_wallet: str) -> List[Dict]:
        """Fetch activity for a single user from API"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {
                "user": proxy_wallet,
                "limit": 500,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return []
            
            activities = response.json()
            
            if not activities:
                return []
            
            # Filter and prepare activities
            filtered_activities = []
            
            for activity in activities:
                # Only store significant activities
                usdc_size = activity.get('usdcSize', 0)
                if usdc_size < self.MIN_TRANSACTION_SIZE:
                    continue
                
                # Add proxy_wallet to activity
                activity['proxyWallet'] = proxy_wallet
                filtered_activities.append(activity)
            
            return filtered_activities
            
        except Exception as e:
            self.logger.debug(f"Error fetching activity for {proxy_wallet}: {e}")
            return []