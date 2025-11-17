from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreSeries(DatabaseManager):
    """Manager for storing series with multithreading support"""

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

    def _store_series_list(self, series_list: List[Dict]):
            """
            Store multiple series records
            """
            series_records = []
            
            for series in series_list:
                record = self._prepare_series_record(series)
                series_records.append(record)
                
                # Process events if present
                if 'events' in series:
                    self._store_series_events(series['id'], series['events'])
                
                # Process collections if present
                if 'collections' in series:
                    self._store_series_collections(series['id'], series['collections'])
            
            # Bulk insert series
            if series_records:
                self.bulk_insert_or_replace('series', series_records)
                self.logger.debug(f"Stored {len(series_records)} series")
    
    def _store_series_detailed(self, series: Dict):
        """
        Store detailed series information
        Thread-safe when called with _db_lock
        """
        record = self._prepare_series_record(series)
        with self._db_lock:
            self.insert_or_replace('series', record)
        self.logger.debug(f"Stored detailed series: {series.get('id')}")
    
    def _prepare_series_record(self, series: Dict) -> Dict:
        """
        Prepare a series record for database insertion
        """
        return {
            'id': series.get('id'),
            'ticker': series.get('ticker'),
            'slug': series.get('slug'),
            'title': series.get('title'),
            'subtitle': series.get('subtitle'),
            'series_type': series.get('seriesType'),
            'recurrence': series.get('recurrence'),
            'description': series.get('description'),
            'image': series.get('image'),
            'icon': series.get('icon'),
            'layout': series.get('layout'),
            'active': series.get('active'),
            'closed': series.get('closed'),
            'archived': series.get('archived'),
            'new': series.get('new'),
            'featured': series.get('featured'),
            'restricted': series.get('restricted'),
            'is_template': series.get('isTemplate'),
            'template_variables': json.dumps(series.get('templateVariables')) if series.get('templateVariables') else None,
            'published_at': series.get('publishedAt'),
            'created_by': series.get('createdBy'),
            'updated_by': series.get('updatedBy'),
            'created_at': series.get('createdAt'),
            'updated_at': series.get('updatedAt'),
            'comments_enabled': series.get('commentsEnabled'),
            'competitive': series.get('competitive'),
            'volume_24hr': series.get('volume24hr'),
            'volume': series.get('volume'),
            'liquidity': series.get('liquidity'),
            'start_date': series.get('startDate'),
            'pyth_token_id': series.get('pythTokenID'),
            'cg_asset_name': series.get('cgAssetName'),
            'score': series.get('score'),
            'fetched_at': datetime.now().isoformat()
        }
    
    def _store_series_events(self, series_id: str, events: List[Dict]):
        """
        Store events associated with a series
        Thread-safe when called with _db_lock
        """
        event_records = []

        for event in events:
            # Store event data
            event_record = {
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
                'active': event.get('active'),
                'closed': event.get('closed'),
                'archived': event.get('archived'),
                'new': event.get('new'),
                'featured': event.get('featured'),
                'restricted': event.get('restricted'),
                'competitive': event.get('competitive'),
                'enable_order_book': event.get('enableOrderBook'),
                'created_at': event.get('createdAt'),
                'updated_at': event.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            event_records.append(event_record)

            # Store event tags if present
            if 'tags' in event and isinstance(event['tags'], list):
                self._store_event_tags(event['id'], event['tags'])

        # Bulk insert events
        if event_records:
            with self._db_lock:
                self.bulk_insert_or_replace('events', event_records)

        # Store series-event relationship
        with self._db_lock:
            conn = self.get_persistent_connection()
            cursor = conn.cursor()

            event_ids = [event.get('id') for event in events]

            cursor.execute("SELECT event_ids FROM series_events WHERE series_id = ?", (series_id,))
            result = cursor.fetchone()

            if result:
                existing_event_ids = json.loads(result[0])
                for event_id in event_ids:
                    if event_id not in existing_event_ids:
                        existing_event_ids.append(event_id)
                cursor.execute(
                    "UPDATE series_events SET event_ids = ? WHERE series_id = ?",
                    (json.dumps(existing_event_ids), series_id)
                )
            else:
                cursor.execute(
                    "INSERT INTO series_events (series_id, event_ids) VALUES (?, ?)",
                    (series_id, json.dumps(event_ids))
                )

            conn.commit()

        self.logger.debug(f"Stored {len(events)} events for series {series_id}")

    def _store_series_collections(self, series_id: str, collections: List[Dict]):
        """
        Store collections associated with a series
        Thread-safe when called with _db_lock
        """
        collection_records = []
        relationships = []

        for collection in collections:
            # Store collection data
            collection_record = {
                'id': collection.get('id'),
                'ticker': collection.get('ticker'),
                'slug': collection.get('slug'),
                'title': collection.get('title'),
                'subtitle': collection.get('subtitle'),
                'collection_type': collection.get('collectionType'),
                'description': collection.get('description'),
                'tags': json.dumps(collection.get('tags')) if collection.get('tags') else None,
                'image': collection.get('image'),
                'icon': collection.get('icon'),
                'header_image': collection.get('headerImage'),
                'layout': collection.get('layout'),
                'active': collection.get('active'),
                'closed': collection.get('closed'),
                'archived': collection.get('archived'),
                'new': collection.get('new'),
                'featured': collection.get('featured'),
                'restricted': collection.get('restricted'),
                'is_template': collection.get('isTemplate'),
                'template_variables': json.dumps(collection.get('templateVariables')) if collection.get('templateVariables') else None,
                'published_at': collection.get('publishedAt'),
                'created_by': collection.get('createdBy'),
                'updated_by': collection.get('updatedBy'),
                'created_at': collection.get('createdAt'),
                'updated_at': collection.get('updatedAt'),
                'comments_enabled': collection.get('commentsEnabled'),
                'comment_count': collection.get('commentCount'),
                'fetched_at': datetime.now().isoformat()
            }
            collection_records.append(collection_record)

            # Create series-collection relationship
            relationship = {
                'series_id': series_id,
                'collection_id': collection.get('id')
            }
            relationships.append(relationship)

            # Store collection tags if present
            if 'tags' in collection and isinstance(collection['tags'], list):
                self._store_collection_tags(collection['id'], collection['tags'])

        # Bulk insert with lock
        if collection_records:
            with self._db_lock:
                self.bulk_insert_or_replace('collections', collection_records)
        if relationships:
            with self._db_lock:
                self.bulk_insert_or_replace('series_collections', relationships)

        self.logger.debug(f"Stored {len(collections)} collections for series {series_id}")