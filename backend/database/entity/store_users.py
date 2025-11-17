from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreUsers(DatabaseManager):
    """Manager for storing users with multithreading support"""

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


    def _bulk_insert_activities(self, activities: List[Dict]):
            """Bulk insert activities into database"""
            if not activities:
                return
            
            # Prepare data for bulk insert
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
            
            # Bulk insert with INSERT OR IGNORE to avoid duplicates
            self.bulk_insert_or_ignore('user_activity', activity_data, batch_size=100)
            self.logger.info(f"Bulk inserted {len(activity_data)} activities")




    def _bulk_insert_values(self, values: List[Dict]):
        """Bulk insert portfolio values"""
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


    def _store_user_activity(self, proxy_wallet: str, activity: List[Dict]):
        """Store user activity (thread-safe when called with _db_lock)"""
        activity_records = []
        
        for act in activity:
            record = {
                'proxy_wallet': proxy_wallet,
                'timestamp': act.get('timestamp'),
                'condition_id': act.get('conditionId'),
                'transaction_hash': act.get('transactionHash'),
                'type': act.get('type'),
                'side': act.get('side'),
                'size': act.get('size'),
                'usdc_size': act.get('usdcSize'),
                'price': act.get('price'),
                'asset': act.get('asset'),
                'outcome_index': act.get('outcomeIndex'),
                'title': act.get('title'),
                'slug': act.get('slug'),
                'event_slug': act.get('eventSlug'),
                'outcome': act.get('outcome'),
                'username': act.get('username'),
                'pseudonym': act.get('pseudonym'),
                'bio': act.get('bio'),
                'profile_image': act.get('profileImage')
            }
            activity_records.append(record)
        
        if activity_records:
            self.bulk_insert_or_replace('user_activity', activity_records)