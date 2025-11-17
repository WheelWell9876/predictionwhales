"""
Transactions Manager for Polymarket Terminal - ENHANCED WHALE FOCUSED
Handles fetching whale transactions and comprehensive user trading data
With improved thread safety and batch processing to avoid database locks
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from .database.database_manager import DatabaseManager

class TransactionsManager(DatabaseManager):
    """Enhanced manager for whale transaction and user trading data operations"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
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

    def fetch_comprehensive_whale_data(self, limit_users: int = None) -> Dict[str, int]:
        """
        Main entry point for fetching comprehensive whale transaction and trading data
        This includes: transactions, positions, activity, portfolio values, and closed positions
        """
        self.logger.info("=" * 60)
        self.logger.info("🐋 STARTING COMPREHENSIVE WHALE DATA FETCH")
        self.logger.info("=" * 60)
        
        results = {
            'whale_users': 0,
            'transactions': 0,
            'current_positions': 0,
            'closed_positions': 0,
            'user_activities': 0,
            'portfolio_values': 0,
            'trades': 0
        }
        
        # Step 1: Get whale users from various sources
        whale_users = self._get_whale_users(limit_users)
        results['whale_users'] = len(whale_users)
        
        if not whale_users:
            self.logger.warning("No whale users found to process")
            return results
        
        self.logger.info(f"Processing {len(whale_users)} whale users...")
        
        # Step 2: Fetch current positions for whales
        self.logger.info("\n📊 Phase 1: Fetching Current Positions...")
        positions_result = self.fetch_user_positions_batch(whale_users)
        results['current_positions'] = positions_result['total_positions']
        
        # Step 3: Fetch user trading activity (with batching)
        self.logger.info("\n📈 Phase 2: Fetching User Activity...")
        activity_result = self.fetch_user_activity_batch(whale_users)
        results['user_activities'] = activity_result['total_activities']
        
        # Step 4: Fetch portfolio values
        self.logger.info("\n💰 Phase 3: Fetching Portfolio Values...")
        values_result = self.fetch_user_values_batch(whale_users)
        results['portfolio_values'] = values_result['values_fetched']
        
        # Step 5: Fetch closed positions
        self.logger.info("\n💸 Phase 4: Fetching Closed Positions...")
        closed_result = self.fetch_closed_positions_batch(whale_users)
        results['closed_positions'] = closed_result['total_positions']
        
        # Step 6: Fetch trades for whales and top markets
        self.logger.info("\n📈 Phase 5: Fetching Whale Trades...")
        trades_result = self.fetch_user_trades_batch(whale_users[:200])  # Limit to top 200 users
        results['trades'] = trades_result['total_trades']
        
        # Step 7: Fetch whale transactions from recent activity
        self.logger.info("\n💸 Phase 6: Fetching Recent Whale Transactions...")
        tx_result = self.fetch_recent_whale_transactions()
        results['transactions'] = tx_result
        
        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("✅ COMPREHENSIVE WHALE DATA FETCH COMPLETE!")
        self.logger.info("=" * 60)
        for key, value in results.items():
            self.logger.info(f"   {key:<20} {value:>10,}")
        
        return results

    def _get_whale_users(self, limit: int = None) -> List[str]:
        """Get unique whale users from various sources"""
        query = """
            SELECT DISTINCT proxy_wallet
            FROM (
                -- Users marked as whales
                SELECT proxy_wallet FROM users WHERE is_whale = 1
                UNION
                -- Users with large transactions
                SELECT DISTINCT proxy_wallet FROM transactions WHERE usdc_size >= ?
                UNION
                -- Users with large positions in market_holders
                SELECT DISTINCT proxy_wallet FROM market_holders WHERE amount >= ?
                UNION
                -- Users with significant trading activity
                SELECT DISTINCT proxy_wallet FROM user_trades WHERE size >= ?
            )
            ORDER BY proxy_wallet
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        users = self.fetch_all(query, (
            self.MIN_TRANSACTION_SIZE,
            self.MIN_TRANSACTION_SIZE / 2,  # Lower threshold for holders
            self.MIN_TRANSACTION_SIZE / 2
        ))
        
        return [u['proxy_wallet'] for u in users]


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
                self._bulk_insert_transactions(all_transactions)
            
            self.logger.info(f"Fetched {len(all_transactions)} whale transactions")
            return len(all_transactions)
            
        except Exception as e:
            self.logger.error(f"Error fetching whale transactions: {e}")
            return 0


