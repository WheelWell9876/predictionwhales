"""
ID transactions
Handles individual fetching for the transactions
"""

from typing import Dict, List
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_transactions import StoreTransactionsManager

class IdTransactionsManager(DatabaseManager):
    """Manager for individual transaction fetching"""

    def __init__(self):
        super().__init__()
        self.config = Config
        self.clob_api_url = Config.CLOB_API_URL if Config.CLOB_API_URL else "https://clob.polymarket.com"
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreTransactionsManager()
        
        # Reduce max workers to avoid overwhelming the database
        self.max_workers = min(5, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 5))
        
        # Thread-safe batch collections
        self._batch_lock = Lock()
        self._batch_data = defaultdict(list)
        
        # Progress tracking
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Whale thresholds
        self.MIN_TRANSACTION_SIZE = Config.MIN_TRANSACTION_SIZE if hasattr(Config, 'MIN_TRANSACTION_SIZE') else 500
        self.MIN_WHALE_TRADE = Config.MIN_WHALE_TRADE if hasattr(Config, 'MIN_WHALE_TRADE') else 10000
        self.MIN_POSITION_VALUE = 500

    def _fetch_user_trades_api(self, proxy_wallet: str) -> Dict:
        """Fetch trades for a single user from API"""
        try:
            url = f"{self.data_api_url}/trades"
            params = {
                "user": proxy_wallet,
                "limit": "100",
                "takerOnly": "false"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'trades': [], 'whale_trades': []}
            
            trades = response.json()
            
            if not trades:
                return {'trades': [], 'whale_trades': []}
            
            processed_trades = []
            whale_trades = []
            
            for trade in trades:
                trade_value = trade.get('size', 0) * trade.get('price', 0)
                
                trade['proxyWallet'] = proxy_wallet
                processed_trades.append(trade)
                
                # Track whale trades
                if trade_value > 10000:
                    whale_trades.append({
                        'wallet': proxy_wallet,
                        'value': trade_value,
                        'side': trade.get('side')
                    })
            
            return {'trades': processed_trades, 'whale_trades': whale_trades}
            
        except Exception as e:
            self.logger.debug(f"Error fetching trades for {proxy_wallet}: {e}")
            return {'trades': [], 'whale_trades': []}

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