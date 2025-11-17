from datetime import datetime
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreEvents(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

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


    def _store_events(self, events: List[Dict]):
            """
            Store multiple events in the database (thread-safe)
            """
            event_records = []
            
            for event in events:
                record = {
                    'id': event.get('id'),
                    'ticker': event.get('ticker'),
                    'slug': event.get('slug'),
                    'title': event.get('title'),
                    'description': event.get('description'),
                    'start_date': event.get('startDate'),
                    'creation_date': event.get('creationDate'),
                    'end_date': event.get('endDate'),
                    'image': event.get('image'),
                    'icon': event.get('icon'),
                    'liquidity': event.get('liquidity'),
                    'liquidity_clob': event.get('liquidityClob'),
                    'volume': event.get('volume'),
                    'volume_clob': event.get('volumeClob'),
                    'volume_24hr': event.get('volume24hr'),
                    'volume_24hr_clob': event.get('volume24hrClob'),
                    'volume_1wk': event.get('volume1wk'),
                    'volume_1wk_clob': event.get('volume1wkClob'),
                    'volume_1mo': event.get('volume1mo'),
                    'volume_1mo_clob': event.get('volume1moClob'),
                    'volume_1yr': event.get('volume1yr'),
                    'volume_1yr_clob': event.get('volume1yrClob'),
                    'open_interest': event.get('openInterest'),
                    'competitive': event.get('competitive'),
                    'comment_count': event.get('commentCount'),
                    'active': event.get('active'),
                    'closed': event.get('closed'),
                    'archived': event.get('archived'),
                    'new': event.get('new'),
                    'featured': event.get('featured'),
                    'restricted': event.get('restricted'),
                    'enable_order_book': event.get('enableOrderBook'),
                    'cyom': event.get('cyom'),
                    'show_all_outcomes': event.get('showAllOutcomes'),
                    'show_market_images': event.get('showMarketImages'),
                    'enable_neg_risk': event.get('enableNegRisk'),
                    'automatically_active': event.get('automaticallyActive'),
                    'neg_risk_augmented': event.get('negRiskAugmented'),
                    'pending_deployment': event.get('pendingDeployment'),
                    'deploying': event.get('deploying'),
                    'created_at': event.get('createdAt'),
                    'updated_at': event.get('updatedAt'),
                    'fetched_at': datetime.now().isoformat()
                }
                event_records.append(record)
                
                # Store basic tags if present
                if 'tags' in event:
                    self._store_event_tags_basic(event['id'], event['tags'])
            
            # Bulk insert events
            if event_records:
                self.bulk_insert_or_replace('events', event_records)
    
    def _store_event_detailed(self, event: Dict):
        """
        Store detailed event information
        """
        record = {
            'id': event.get('id'),
            'ticker': event.get('ticker'),
            'slug': event.get('slug'),
            'title': event.get('title'),
            'description': event.get('description'),
            'start_date': event.get('startDate'),
            'creation_date': event.get('creationDate'),
            'end_date': event.get('endDate'),
            'image': event.get('image'),
            'icon': event.get('icon'),
            'liquidity': event.get('liquidity'),
            'liquidity_clob': event.get('liquidityClob'),
            'volume': event.get('volume'),
            'volume_clob': event.get('volumeClob'),
            'volume_24hr': event.get('volume24hr'),
            'volume_24hr_clob': event.get('volume24hrClob'),
            'volume_1wk': event.get('volume1wk'),
            'volume_1wk_clob': event.get('volume1wkClob'),
            'volume_1mo': event.get('volume1mo'),
            'volume_1mo_clob': event.get('volume1moClob'),
            'volume_1yr': event.get('volume1yr'),
            'volume_1yr_clob': event.get('volume1yrClob'),
            'open_interest': event.get('openInterest'),
            'competitive': event.get('competitive'),
            'comment_count': event.get('commentCount'),
            'active': event.get('active'),
            'closed': event.get('closed'),
            'archived': event.get('archived'),
            'new': event.get('new'),
            'featured': event.get('featured'),
            'restricted': event.get('restricted'),
            'enable_order_book': event.get('enableOrderBook'),
            'cyom': event.get('cyom'),
            'show_all_outcomes': event.get('showAllOutcomes'),
            'show_market_images': event.get('showMarketImages'),
            'enable_neg_risk': event.get('enableNegRisk'),
            'automatically_active': event.get('automaticallyActive'),
            'neg_risk_augmented': event.get('negRiskAugmented'),
            'pending_deployment': event.get('pendingDeployment'),
            'deploying': event.get('deploying'),
            'created_at': event.get('createdAt'),
            'updated_at': event.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }
        
        self.insert_or_replace('events', record)
        self.logger.debug(f"Stored detailed event: {event.get('id')}")