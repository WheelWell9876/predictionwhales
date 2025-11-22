"""
Series Manager for Polymarket Terminal
Handles series data from events and standalone series operations
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_series import StoreSeriesManager
from backend.fetch.entity.batch.batch_series import BatchSeriesManager
from backend.fetch.entity.id.id_series import IdSeriesManager

class SeriesManager:
    """Manager for series-related operations"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize database and storage
        self.db_manager = DatabaseManager()
        self.store_manager = StoreSeriesManager()
        
        # Initialize fetchers
        self.batch_fetcher = BatchSeriesManager()
        self.id_fetcher = IdSeriesManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
    
    def fetch_all_series(self, num_threads: int = 10) -> Dict:
        """
        Fetch all series from the API and store with JSON relationships
        
        Args:
            num_threads: Number of concurrent threads
            
        Returns:
            Dictionary with results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📚 Fetching ALL SERIES")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            all_series = []
            offset = 0
            limit = 100
            
            self.logger.info("Starting to fetch all series...")
            
            while True:
                try:
                    url = f"{self.base_url}/series"
                    params = {
                        "limit": limit,
                        "offset": offset,
                        "order": "volume",
                        "ascending": "false"
                    }
                    
                    response = requests.get(
                        url,
                        params=params,
                        headers=self.config.get_api_headers(),
                        timeout=self.config.REQUEST_TIMEOUT
                    )
                    response.raise_for_status()
                    
                    series_list = response.json()
                    
                    if not series_list:
                        break
                    
                    all_series.extend(series_list)
                    
                    self.logger.info(f"Fetched {len(series_list)} series (offset: {offset})")
                    
                    offset += limit
                    time.sleep(self.config.RATE_LIMIT_DELAY)
                    
                    if len(series_list) < limit:
                        break
                        
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error fetching series at offset {offset}: {e}")
                    break
            
            # Process and store series with JSON relationships
            if all_series:
                self._process_and_store_series(all_series)
                result['count'] = len(all_series)
                result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Series loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading series: {e}")
            
        return result
    
    def _process_and_store_series(self, series_list: List[Dict]):
        """
        Process and store series with JSON relationships
        
        Args:
            series_list: List of series dictionaries from API
        """
        series_records = []
        series_events_records = []
        series_tags_records = []
        series_categories_records = []
        series_collections_records = []
        series_chats_records = []
        
        for series in series_list:
            series_id = series.get('id')
            if not series_id:
                continue
            
            # Prepare main series record
            series_record = {
                'id': series_id,
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
                'active': int(series.get('active', True)),
                'closed': int(series.get('closed', False)),
                'archived': int(series.get('archived', False)),
                'new': int(series.get('new', False)),
                'featured': int(series.get('featured', False)),
                'restricted': int(series.get('restricted', False)),
                'is_template': int(series.get('isTemplate', False)),
                'template_variables': int(series.get('templateVariables', False)),
                'published_at': series.get('publishedAt'),
                'created_by': series.get('createdBy'),
                'updated_by': series.get('updatedBy'),
                'created_at': series.get('createdAt'),
                'updated_at': series.get('updatedAt'),
                'comments_enabled': int(series.get('commentsEnabled', False)),
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
            series_records.append(series_record)
            
            # Extract event IDs and store as JSON
            if 'events' in series and series['events']:
                event_ids = []
                for event in series['events']:
                    if isinstance(event, dict) and event.get('id'):
                        event_ids.append(event['id'])
                    elif isinstance(event, str):
                        event_ids.append(event)
                
                if event_ids:
                    series_events_records.append({
                        'series_id': series_id,
                        'event_ids': json.dumps(event_ids)
                    })
            
            # Extract tag IDs and store as JSON
            if 'tags' in series and series['tags']:
                tag_ids = []
                for tag in series['tags']:
                    if isinstance(tag, dict) and tag.get('id'):
                        tag_ids.append(tag['id'])
                    elif isinstance(tag, str):
                        tag_ids.append(tag)
                
                if tag_ids:
                    series_tags_records.append({
                        'series_id': series_id,
                        'tag_ids': json.dumps(tag_ids)
                    })
            
            # Extract category IDs and store as JSON
            if 'categories' in series and series['categories']:
                category_ids = []
                for cat in series['categories']:
                    if isinstance(cat, dict) and cat.get('id'):
                        category_ids.append(cat['id'])
                    elif isinstance(cat, str):
                        category_ids.append(cat)
                
                if category_ids:
                    series_categories_records.append({
                        'series_id': series_id,
                        'category_ids': json.dumps(category_ids)
                    })
            
            # Extract collection IDs and store as JSON
            if 'collections' in series and series['collections']:
                collection_ids = []
                for col in series['collections']:
                    if isinstance(col, dict) and col.get('id'):
                        collection_ids.append(col['id'])
                    elif isinstance(col, str):
                        collection_ids.append(col)
                
                if collection_ids:
                    series_collections_records.append({
                        'series_id': series_id,
                        'collection_ids': json.dumps(collection_ids)
                    })
            
            # Extract chat IDs and store as JSON
            if 'chats' in series and series['chats']:
                chat_ids = []
                for chat in series['chats']:
                    if isinstance(chat, dict) and chat.get('id'):
                        chat_ids.append(chat['id'])
                    elif isinstance(chat, str):
                        chat_ids.append(chat)
                
                if chat_ids:
                    series_chats_records.append({
                        'series_id': series_id,
                        'chat_ids': json.dumps(chat_ids)
                    })
        
        # Store all data
        if series_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series', series_records)
                self.logger.info(f"Stored {len(series_records)} series")
        
        if series_events_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series_events', series_events_records)
                self.logger.info(f"Stored {len(series_events_records)} series-events relationships")
        
        if series_tags_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series_tags', series_tags_records)
                self.logger.info(f"Stored {len(series_tags_records)} series-tags relationships")
        
        if series_categories_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series_categories', series_categories_records)
                self.logger.info(f"Stored {len(series_categories_records)} series-categories relationships")
        
        if series_collections_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series_collections', series_collections_records)
                self.logger.info(f"Stored {len(series_collections_records)} series-collections relationships")
        
        if series_chats_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series_chats', series_chats_records)
                self.logger.info(f"Stored {len(series_chats_records)} series-chats relationships")
    
    def load_series_only(self) -> Dict:
        """
        Load series data - callable from data_fetcher
        
        Returns:
            Dictionary with load results
        """
        return self.fetch_all_series(num_threads=10)
    
    def delete_series_only(self) -> Dict:
        """Delete series data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting SERIES Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Get current count
            before_count = self.db_manager.get_table_count('series')
            
            # Delete all related data
            self.db_manager.delete_records('series_events', commit=False)
            self.db_manager.delete_records('series_tags', commit=False)
            self.db_manager.delete_records('series_categories', commit=False)
            self.db_manager.delete_records('series_collections', commit=False)
            self.db_manager.delete_records('series_chats', commit=False)
            deleted = self.db_manager.delete_records('series', commit=True)
            
            result['deleted'] = before_count
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} series and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting series: {e}")
            
        return result
    
    def store_event_series(self, event_id: str, series_data: List):
        """
        Store the relationship between an event and its series
        Delegates to store_manager

        Args:
            event_id: The event ID
            series_data: List of series dictionaries or IDs from the API
        """
        self.store_manager.store_event_series(event_id, series_data)

    def get_series_statistics(self) -> Dict:
        """Get statistics about series in the database"""
        stats = {}

        # Total series
        total = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM series")
        stats['total_series'] = total['count'] if total else 0

        # Active series
        active = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM series WHERE active = 1")
        stats['active_series'] = active['count'] if active else 0

        # Series with events
        with_events = self.db_manager.fetch_one("""
            SELECT COUNT(*) as count FROM series_events
        """)
        stats['series_with_events'] = with_events['count'] if with_events else 0

        return stats