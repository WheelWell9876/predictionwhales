"""
Store Transactions
Handles storage functionality for transactions data
"""

from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreTransactionsManager(DatabaseManager):
    """Manager for storing transaction data with thread-safe operations"""

    def __init__(self):
        super().__init__()
        from backend.config import Config
        self.config = Config
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()

    def _bulk_insert_transactions(self, transactions: List[Dict]):
        """Bulk insert transactions (thread-safe)"""
        if not transactions:
            return
        
        tx_data = []
        
        for tx in transactions:
            usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
            
            tx_data.append({
                'transaction_hash': tx.get('transactionHash'),
                'proxy_wallet': tx.get('proxyWallet'),
                'timestamp': tx.get('timestamp'),
                'market_id': tx.get('marketId'),
                'condition_id': tx.get('conditionId'),
                'side': tx.get('side'),
                'size': tx.get('size'),
                'price': tx.get('price'),
                'usdc_size': usdc_size,
                'type': tx.get('type', 'trade'),
                'username': tx.get('name'),
                'pseudonym': tx.get('pseudonym'),
                'is_whale': tx.get('is_whale', 0)
            })
        
        with self._db_lock:
            self.bulk_insert_or_replace('transactions', tx_data, batch_size=100)
            self.logger.info(f"Bulk inserted {len(tx_data)} transactions")

    def _bulk_insert_trades(self, trades: List[Dict]):
        """Bulk insert trades (thread-safe)"""
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
        
        with self._db_lock:
            self.bulk_insert_or_ignore('user_trades', trade_data, batch_size=100)
            self.logger.info(f"Bulk inserted {len(trade_data)} trades")

    def _store_user_trades(self, proxy_wallet: str, trades: List[Dict]):
        """Store user trades (thread-safe)"""
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
            with self._db_lock:
                self.bulk_insert_or_replace('user_trades', trade_records)







    def _bulk_insert_activities(self, activities: List[Dict]):
        """Bulk insert activities into database (thread-safe)"""
        if not activities:
            return
        
        activity_data = []
        
        for activity in activities:
            activity_data.append({
                'proxy_wallet': activity.get('proxyWallet'),
                'timestamp': activity.get('timestamp'),
                'condition_id': activity.get('conditionId'),
                'transaction_hash': activity.get('transactionHash'),
                'type': activity.get('type'),
                'side': activity.get('side'),
                'size': activity.get('size'),
                'usdc_size': activity.get('usdcSize'),
                'price': activity.get('price'),
                'asset': activity.get('asset'),
                'outcome_index': activity.get('outcomeIndex'),
                'title': activity.get('title'),
                'slug': activity.get('slug'),
                'event_slug': activity.get('eventSlug'),
                'outcome': activity.get('outcome'),
                'username': activity.get('name'),
                'pseudonym': activity.get('pseudonym'),
                'bio': activity.get('bio'),
                'profile_image': activity.get('profileImage')
            })
        
        with self._db_lock:
            self.bulk_insert_or_ignore('user_activity', activity_data, batch_size=100)
            self.logger.info(f"Bulk inserted {len(activity_data)} activities")

    def _bulk_insert_values(self, values: List[Dict]):
        """Bulk insert portfolio values (thread-safe)"""
        if not values:
            return
        
        value_data = []
        user_updates = []
        
        for val in values:
            if val['fetched']:
                value_data.append({
                    'proxy_wallet': val['wallet'],
                    'market_condition_id': None,
                    'value': val['value']
                })
                
                user_updates.append({
                    'proxy_wallet': val['wallet'],
                    'total_value': val['value'],
                    'last_updated': datetime.now()
                })
        
        # Bulk insert values
        if value_data:
            with self._db_lock:
                self.bulk_insert_or_replace('user_values', value_data, batch_size=100)
                
                # Update users table
                for update in user_updates:
                    self.execute_query("""
                        UPDATE users SET total_value = ?, last_updated = ?
                        WHERE proxy_wallet = ?
                    """, (update['total_value'], update['last_updated'], update['proxy_wallet']), commit=False)
                
                # Commit all updates
                conn = self.get_connection()
                conn.commit()
            
        self.logger.info(f"Bulk inserted {len(value_data)} portfolio values")