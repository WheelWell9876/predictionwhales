from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreTransactions(DatabaseManager):
    """Manager for storing transactions with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ...config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0


    def _bulk_insert_trades(self, trades: List[Dict]):
            """Bulk insert trades"""
            if not trades:
                return
            
            trade_data = []
            
            for trade in trades:
                trade_data.append({
                    'proxy_wallet': trade.get('proxyWallet'),
                    'side': trade.get('side'),
                    'asset': trade.get('asset'),
                    'condition_id': trade.get('conditionId'),
                    'size': trade.get('size'),
                    'price': trade.get('price'),
                    'timestamp': trade.get('timestamp'),
                    'transaction_hash': trade.get('transactionHash'),
                    'title': trade.get('title'),
                    'slug': trade.get('slug'),
                    'icon': trade.get('icon'),
                    'event_slug': trade.get('eventSlug'),
                    'outcome': trade.get('outcome'),
                    'outcome_index': trade.get('outcomeIndex'),
                    'username': trade.get('name'),
                    'pseudonym': trade.get('pseudonym'),
                    'bio': trade.get('bio'),
                    'profile_image': trade.get('profileImage'),
                    'profile_image_optimized': trade.get('profileImageOptimized')
                })
            
            self.bulk_insert_or_ignore('user_trades', trade_data, batch_size=100)
            self.logger.info(f"Bulk inserted {len(trade_data)} trades")



    def _store_user_trades(self, proxy_wallet: str, trades: List[Dict]):
        """Store user trades (thread-safe when called with _db_lock)"""
        trade_records = []
        
        for trade in trades:
            record = {
                'proxy_wallet': proxy_wallet,
                'side': trade.get('side'),
                'asset': trade.get('asset'),
                'condition_id': trade.get('conditionId'),
                'size': trade.get('size'),
                'price': trade.get('price'),
                'timestamp': trade.get('timestamp'),
                'transaction_hash': trade.get('transactionHash'),
                'title': trade.get('title'),
                'slug': trade.get('slug'),
                'icon': trade.get('icon'),
                'event_slug': trade.get('eventSlug'),
                'outcome': trade.get('outcome'),
                'outcome_index': trade.get('outcomeIndex'),
                'username': trade.get('username'),
                'pseudonym': trade.get('pseudonym'),
                'bio': trade.get('bio'),
                'profile_image': trade.get('profileImage'),
                'profile_image_optimized': trade.get('profileImageOptimized')
            }
            trade_records.append(record)
        
        if trade_records:
            self.bulk_insert_or_replace('user_trades', trade_records)



    # def _bulk_insert_transactions(self, transactions: List[Dict]):
    #     """Bulk insert transactions"""
    #     if not transactions:
    #         return
        
    #     tx_data = []
        
    #     for tx in transactions:
    #         usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
            
    #         tx_data.append({
    #             'transaction_hash': tx.get('transactionHash'),
    #             'proxy_wallet': tx.get('proxyWallet'),
    #             'timestamp': tx.get('timestamp'),
    #             'market_id': tx.get('marketId'),
    #             'condition_id': tx.get('conditionId'),
    #             'side': tx.get('side'),
    #             'size': tx.get('size'),
    #             'price': tx.get('price'),
    #             'usdc_size': usdc_size,
    #             'type': tx.get('type', 'trade'),
    #             'username': tx.get('name'),
    #             'pseudonym': tx.get('pseudonym'),
    #             'is_whale': tx.get('is_whale', 0)
    #         })
        
    #     self.bulk_insert_or_replace('transactions', tx_data, batch_size=100)
    #     self.logger.info(f"Bulk inserted {len(tx_data)} transactions")
