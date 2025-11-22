"""
Store Series
Handles storage functionality for series data
"""

from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreSeriesManager(DatabaseManager):
    """Manager for storing series data with thread-safe operations"""

    def __init__(self):
        super().__init__()
        from backend.config import Config
        self.config = Config
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()

    def _store_series_list(self, series_list: List[Dict]):
        """
        Store multiple series in the database (thread-safe)
        """
        series_records = []
        
        for series in series_list:
            record = {
                'id': series.get('id'),
                'slug': series.get('slug'),
                'title': series.get('title'),
                'description': series.get('description'),
                'creator': series.get('creator'),
                'liquidity': series.get('liquidity'),
                'volume': series.get('volume'),
                'volume_24hr': series.get('volume24hr'),
                'volume_1wk': series.get('volume1wk'),
                'volume_1mo': series.get('volume1mo'),
                'volume_1yr': series.get('volume1yr'),
                'open_interest': series.get('openInterest'),
                'avg_price': series.get('avgPrice'),
                'active': series.get('active'),
                'closed': series.get('closed'),
                'archived': series.get('archived'),
                'new': series.get('new'),
                'featured': series.get('featured'),
                'restricted': series.get('restricted'),
                'created_at': series.get('createdAt'),
                'updated_at': series.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            series_records.append(record)
        
        if series_records:
            with self._db_lock:
                self.bulk_insert_or_replace('series', series_records)
                self.logger.debug(f"Stored {len(series_records)} series")

    def _store_series_detailed(self, series: Dict):
        """
        Store detailed series information (thread-safe)
        """
        record = {
            'id': series.get('id'),
            'slug': series.get('slug'),
            'title': series.get('title'),
            'description': series.get('description'),
            'creator': series.get('creator'),
            'liquidity': series.get('liquidity'),
            'volume': series.get('volume'),
            'volume_24hr': series.get('volume24hr'),
            'volume_1wk': series.get('volume1wk'),
            'volume_1mo': series.get('volume1mo'),
            'volume_1yr': series.get('volume1yr'),
            'open_interest': series.get('openInterest'),
            'avg_price': series.get('avgPrice'),
            'active': series.get('active'),
            'closed': series.get('closed'),
            'archived': series.get('archived'),
            'new': series.get('new'),
            'featured': series.get('featured'),
            'restricted': series.get('restricted'),
            'created_at': series.get('createdAt'),
            'updated_at': series.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }
        
        with self._db_lock:
            self.insert_or_replace('series', record)
            self.logger.debug(f"Stored detailed series: {series.get('id')}")

    def _store_series_events(self, series_id: str, events: List[Dict]):
        """
        Store series-event relationships (thread-safe)
        """
        if not events:
            return
        
        event_records = []
        for event in events:
            record = {
                'series_id': series_id,
                'event_id': event.get('id'),
                'position': event.get('position', 0),
                'created_at': datetime.now().isoformat()
            }
            event_records.append(record)
        
        if event_records:
            with self._db_lock:
                self.bulk_insert_or_replace('series_events', event_records)
                self.logger.debug(f"Stored {len(event_records)} events for series {series_id}")

    def _store_series_collections(self, series_id: str, collections: List[Dict]):
        """
        Store series-collection relationships (thread-safe)
        """
        if not collections:
            return

        collection_records = []
        for collection in collections:
            record = {
                'series_id': series_id,
                'collection_id': collection.get('id'),
                'collection_title': collection.get('title'),
                'position': collection.get('position', 0),
                'created_at': datetime.now().isoformat()
            }
            collection_records.append(record)

        if collection_records:
            with self._db_lock:
                self.bulk_insert_or_replace('series_collections', collection_records)
                self.logger.debug(f"Stored {len(collection_records)} collections for series {series_id}")

    def store_event_series(self, event_id: str, series_data: List):
        """
        Store the relationship between an event and its series (thread-safe)

        Args:
            event_id: The event ID
            series_data: List of series dictionaries or IDs from the API
        """
        if not series_data:
            return

        event_series_records = []

        for series in series_data:
            series_id = None
            if isinstance(series, dict):
                series_id = series.get('id')
            elif isinstance(series, str):
                series_id = series

            if series_id:
                event_series_records.append({
                    'event_id': event_id,
                    'series_id': series_id
                })

        if event_series_records:
            with self._db_lock:
                self.bulk_insert_or_ignore('event_series', event_series_records)
                self.logger.debug(f"Stored {len(event_series_records)} series for event {event_id}")