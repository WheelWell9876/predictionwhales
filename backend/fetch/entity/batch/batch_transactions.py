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
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_transactions import StoreTransactionsManager

class BatchTransactionsManager(DatabaseManager):
    """Manager for batch transaction fetching"""

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

    def fetch_recent_whale_transactions(self) -> int:
        """Fetch recent whale transactions from CLOB API"""
        self.logger.info("Fetching recent whale transactions...")
        
        try:
            # Get recent whale users with activity
            recent_whales = self.fetch_all("""
                SELECT DISTINCT proxy_wallet
                FROM user_activity
                WHERE usdc_size >= ?
                AND timestamp > datetime('now', '-7 days')
                ORDER BY usdc_size DESC
                LIMIT 50
            """, (self.MIN_WHALE_TRADE,))
            
            all_transactions = []
            
            for whale in recent_whales:
                try:
                    # Fetch transactions for this whale
                    url = f"{self.clob_api_url}/trades"
                    params = {
                        "user": whale['proxy_wallet'],
                        "limit": 25,
                        "minSize": self.MIN_TRANSACTION_SIZE
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        transactions = response.json()
                        
                        for tx in transactions:
                            usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
                            
                            if usdc_size >= self.MIN_TRANSACTION_SIZE:
                                tx['is_whale'] = 1 if usdc_size >= self.MIN_WHALE_TRADE else 0
                                all_transactions.append(tx)
                                
                except Exception as e:
                    self.logger.debug(f"Error fetching transactions for whale: {e}")
            
            # Bulk insert transactions
            if all_transactions:
                self.store_manager._bulk_insert_transactions(all_transactions)
            
            self.logger.info(f"Fetched {len(all_transactions)} whale transactions")
            return len(all_transactions)
            
        except Exception as e:
            self.logger.error(f"Error fetching whale transactions: {e}")
            return 0

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
            self.store_manager._bulk_insert_trades(all_trades)
        
        # Also fetch trades for top markets
        market_trades = self._fetch_market_trades()
        
        return {
            'total_trades': len(all_trades) + market_trades,
            'whale_trades': len(whale_trades)
        }

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
            self.store_manager._bulk_insert_trades(all_trades)
        
        return len(all_trades)





    def fetch_user_activity_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch activity for a batch of users"""
        self.logger.info(f"Fetching activity for {len(users)} users...")
        
        all_activities = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
            chunk_size = 50
            for i in range(0, len(users), chunk_size):
                chunk = users[i:i+chunk_size]
                
                futures = {
                    executor.submit(self._fetch_user_activity_api, user): user 
                    for user in chunk
                }
                
                for future in as_completed(futures):
                    try:
                        activities = future.result()
                        all_activities.extend(activities)
                    except Exception as e:
                        self.logger.debug(f"Error in activity batch: {e}")
                
                time.sleep(0.5)
        
        # Bulk insert activities
        if all_activities:
            self.store_manager._bulk_insert_activities(all_activities)
        
        return {'total_activities': len(all_activities)}

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





    def fetch_user_values_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch portfolio values for a batch of users"""
        self.logger.info(f"Fetching portfolio values for {len(users)} users...")
        
        values_fetched = 0
        whale_portfolios = []
        all_values = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_user_value_api, user): user 
                for user in users
            }
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result['fetched']:
                        all_values.append(result)
                        values_fetched += 1
                        if result['value'] > 50000:
                            whale_portfolios.append(result)
                except Exception as e:
                    self.logger.debug(f"Error in values batch: {e}")
        
        # Bulk insert values
        if all_values:
            self.store_manager._bulk_insert_values(all_values)
        
        # Display top whale portfolios
        whale_portfolios.sort(key=lambda x: x['value'], reverse=True)
        if whale_portfolios[:5]:
            self.logger.info("🐋 Top 5 Whale Portfolios:")
            for portfolio in whale_portfolios[:5]:
                self.logger.info(f"   {portfolio['wallet'][:10]}... - ${portfolio['value']:,.2f}")
        
        return {
            'values_fetched': values_fetched,
            'whale_count': len(whale_portfolios)
        }

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