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

class SeriesManager:
    """Manager for series-related operations"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize database and storage
        self.db_manager = DatabaseManager()
        self.store_manager = StoreSeriesManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
    
    def store_event_series(self, event_id: str, series_data: List[Dict]):
        """
        Store series data from an event response
        Called by markets_manager when processing events
        
        Args:
            event_id: The event ID
            series_data: List of series dictionaries from event response
        """
        if not series_data:
            return
        
        series_records = []
        event_series_records = []
        series_tags_to_store = []
        series_categories_to_store = []
        series_collections_to_store = []
        
        for series in series_data:
            # Prepare series record
            series_record = {
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
            
            # Prepare event-series relationship
            if series.get('id'):
                event_series_records.append({
                    'event_id': event_id,
                    'series_id': series.get('id')
                })
                
                # Collect tags for this series
                if 'tags' in series and series['tags']:
                    series_tags_to_store.append((series['id'], series['tags']))
                
                # Collect categories for this series
                if 'categories' in series and series['categories']:
                    series_categories_to_store.append((series['id'], series['categories']))
                
                # Collect collections for this series
                if 'collections' in series and series['collections']:
                    series_collections_to_store.append((series['id'], series['collections']))
        
        # Store series records
        if series_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('series', series_records)
                self.logger.debug(f"Stored {len(series_records)} series for event {event_id}")
        
        # Store event-series relationships
        if event_series_records:
            with self._lock:
                self.db_manager.bulk_insert_or_ignore('event_series', event_series_records)
        
        # Store series tags
        for series_id, tags in series_tags_to_store:
            self._store_series_tags(series_id, tags)
        
        # Store series categories
        for series_id, categories in series_categories_to_store:
            self._store_series_categories(series_id, categories)
        
        # Store series collections
        for series_id, collections in series_collections_to_store:
            self._store_series_collections(series_id, collections)
    
    def _store_series_tags(self, series_id: str, tags: List):
        """Store series-tag relationships"""
        tag_records = []
        series_tag_records = []
        
        for tag in tags:
            if isinstance(tag, dict):
                tag_id = tag.get('id')
                tag_slug = tag.get('slug')
                tag_label = tag.get('label')
                
                # Store the tag
                if tag_id:
                    tag_records.append({
                        'id': tag_id,
                        'label': tag_label,
                        'slug': tag_slug,
                        'force_show': int(tag.get('forceShow', False)),
                        'force_hide': int(tag.get('forceHide', False)),
                        'is_carousel': int(tag.get('isCarousel', False)),
                        'published_at': tag.get('publishedAt'),
                        'created_by': tag.get('createdBy'),
                        'updated_by': tag.get('updatedBy'),
                        'created_at': tag.get('createdAt'),
                        'updated_at': tag.get('updatedAt'),
                        'fetched_at': datetime.now().isoformat()
                    })
            else:
                # Tag is just a string
                tag_id = tag
                tag_slug = tag
            
            if tag_id:
                series_tag_records.append({
                    'series_id': series_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                })
        
        # Insert tags first
        if tag_records:
            self.db_manager.bulk_insert_or_ignore('tags', tag_records)
        
        # Then insert relationships
        if series_tag_records:
            self.db_manager.bulk_insert_or_ignore('series_tags', series_tag_records)
    
    def _store_series_categories(self, series_id: str, categories: List[Dict]):
        """Store series-category relationships"""
        category_records = []
        series_category_records = []
        
        for cat in categories:
            if cat.get('id'):
                # Store category
                category_records.append({
                    'id': cat.get('id'),
                    'label': cat.get('label'),
                    'parent_category': cat.get('parentCategory'),
                    'slug': cat.get('slug'),
                    'published_at': cat.get('publishedAt'),
                    'created_by': cat.get('createdBy'),
                    'updated_by': cat.get('updatedBy'),
                    'created_at': cat.get('createdAt'),
                    'updated_at': cat.get('updatedAt')
                })
                
                # Store relationship
                series_category_records.append({
                    'series_id': series_id,
                    'category_id': cat.get('id')
                })
        
        if category_records:
            self.db_manager.bulk_insert_or_ignore('categories', category_records)
        
        if series_category_records:
            self.db_manager.bulk_insert_or_ignore('series_categories', series_category_records)
    
    def _store_series_collections(self, series_id: str, collections: List[Dict]):
        """Store series-collection relationships"""
        collection_records = []
        series_collection_records = []
        
        for col in collections:
            if col.get('id'):
                # Store collection
                collection_records.append({
                    'id': col.get('id'),
                    'ticker': col.get('ticker'),
                    'slug': col.get('slug'),
                    'title': col.get('title'),
                    'subtitle': col.get('subtitle'),
                    'collection_type': col.get('collectionType'),
                    'description': col.get('description'),
                    'tags': col.get('tags'),
                    'image': col.get('image'),
                    'icon': col.get('icon'),
                    'header_image': col.get('headerImage'),
                    'layout': col.get('layout'),
                    'active': int(col.get('active', True)),
                    'closed': int(col.get('closed', False)),
                    'archived': int(col.get('archived', False)),
                    'new': int(col.get('new', False)),
                    'featured': int(col.get('featured', False)),
                    'restricted': int(col.get('restricted', False)),
                    'is_template': int(col.get('isTemplate', False)),
                    'template_variables': col.get('templateVariables'),
                    'published_at': col.get('publishedAt'),
                    'created_by': col.get('createdBy'),
                    'updated_by': col.get('updatedBy'),
                    'created_at': col.get('createdAt'),
                    'updated_at': col.get('updatedAt'),
                    'comments_enabled': int(col.get('commentsEnabled', False))
                })
                
                # Store relationship
                series_collection_records.append({
                    'series_id': series_id,
                    'collection_id': col.get('id')
                })
        
        if collection_records:
            self.db_manager.bulk_insert_or_ignore('collections', collection_records)
        
        if series_collection_records:
            self.db_manager.bulk_insert_or_ignore('series_collections', series_collection_records)
    
    def load_series_only(self) -> Dict:
        """
        Load standalone series data (for series not attached to events)
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📚 Loading SERIES Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            # Note: This would need a batch_series fetcher implementation
            # For now, returning success with 0 count since series come from events
            result['count'] = 0
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Series processing complete")
            self.logger.info(f"⏱️ Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading series: {e}")
            
        return result
    
    def delete_series_only(self) -> Dict:
        """Delete series data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️ Deleting SERIES Data")
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
            SELECT COUNT(DISTINCT series_id) as count FROM series_events
        """)
        stats['series_with_events'] = with_events['count'] if with_events else 0
        
        return stats