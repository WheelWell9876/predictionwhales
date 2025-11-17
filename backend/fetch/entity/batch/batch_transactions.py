"""
Batch transactions
Handles batch fetching for the transactions
"""

import time
from typing import Dict, List
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from ....database.database_manager import DatabaseManager

class BatchTransactionsManager(DatabaseManager):
    """Enhanced manager for whale transaction and user trading data operations"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.clob_api_url = Config.CLOB_API_URL if Config.CLOB_API_URL else "https://clob.polymarket.com"
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        
        # Reduce max workers to avoid overwhelming the database
        self.max_workers = max_workers or min(5, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 5))
        
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
        self.MIN_POSITION_VALUE = 500  # Minimum closed position value to fetch

    def _fetch_market_trades(self) -> int:
            """Fetch trades for top markets"""
            self.logger.info("Fetching trades for top markets...")
            
            # Get top markets by volume
            markets = self.fetch_all("""
                SELECT DISTINCT condition_id, question
                FROM markets
                WHERE active = 1 AND volume > 100000
                ORDER BY volume DESC
                LIMIT 10
            """)
            
            all_trades = []
            
            for market in markets:
                try:
                    url = f"{self.data_api_url}/trades"
                    params = {
                        "market": market['condition_id'],
                        "limit": "50"
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        trades = response.json()
                        
                        for trade in trades:
                            # Only store significant trades
                            trade_value = trade.get('size', 0) * trade.get('price', 0)
                            if trade_value >= self.MIN_TRANSACTION_SIZE:
                                all_trades.append(trade)
                                
                except Exception as e:
                    self.logger.debug(f"Error fetching market trades: {e}")
            
            # Bulk insert market trades
            if all_trades:
                self._bulk_insert_trades(all_trades)
            
            return len(all_trades)


    def fetch_user_trades_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch trades for a batch of users"""
        self.logger.info(f"Fetching trades for {len(users)} users...")
        
        all_trades = []
        whale_trades = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
            chunk_size = 50
            for i in range(0, len(users), chunk_size):
                chunk = users[i:i+chunk_size]
                
                futures = {
                    executor.submit(self._fetch_user_trades_api, user): user 
                    for user in chunk
                }
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        all_trades.extend(result['trades'])
                        whale_trades.extend(result['whale_trades'])
                    except Exception as e:
                        self.logger.debug(f"Error in trades batch: {e}")
                
                time.sleep(0.5)
        
        # Bulk insert trades
        if all_trades:
            self._bulk_insert_trades(all_trades)
        
        # Also fetch trades for top markets
        market_trades = self._fetch_market_trades()
        
        return {
            'total_trades': len(all_trades) + market_trades,
            'whale_trades': len(whale_trades)
        }