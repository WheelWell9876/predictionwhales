"""
Tags Manager for Polymarket Terminal
Handles tag data from events and standalone tag operations
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
from backend.database.entity.store_tags import StoreTagsManager

class TagsManager:
    """Manager for tag-related operations"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize database and storage
        self.db_manager = DatabaseManager()
        self.store_manager = StoreTagsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
    
    def store_event_tags(self, event_id: str, tags: List):
        """
        Store tags from an event response
        Called by markets_manager when processing events
        
        Args:
            event_id: The event ID
            tags: List of tags from event response (can be strings or dicts)
        """
        if not tags or not self.config.FETCH_TAGS:
            return
        
        tag_records = []
        event_tag_records = []
        
        for tag in tags:
            # Handle both string tags and tag objects
            if isinstance(tag, str):
                tag_id = tag
                tag_slug = tag
                tag_label = tag
                tag_data = {}
            elif isinstance(tag, dict):
                tag_id = tag.get('id', '')
                tag_slug = tag.get('slug', '')
                tag_label = tag.get('label', '')
                tag_data = tag
            else:
                self.logger.warning(f"Skipping unknown tag type: {type(tag)}")
                continue
            
            if tag_id:
                # Prepare tag record
                tag_record = {
                    'id': tag_id,
                    'slug': tag_slug,
                    'label': tag_label,
                    'force_show': int(tag_data.get('forceShow', False)) if tag_data else 0,
                    'force_hide': int(tag_data.get('forceHide', False)) if tag_data else 0,
                    'is_carousel': int(tag_data.get('isCarousel', False)) if tag_data else 0,
                    'published_at': tag_data.get('publishedAt') if tag_data else None,
                    'created_by': tag_data.get('createdBy') if tag_data else None,
                    'updated_by': tag_data.get('updatedBy') if tag_data else None,
                    'created_at': tag_data.get('createdAt') if tag_data else None,
                    'updated_at': tag_data.get('updatedAt') if tag_data else None,
                    'fetched_at': datetime.now().isoformat()
                }
                tag_records.append(tag_record)
                
                # Prepare event-tag relationship
                event_tag_record = {
                    'event_id': event_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                }
                event_tag_records.append(event_tag_record)
        
        # Store tags (ignore duplicates)
        if tag_records:
            with self._lock:
                self.db_manager.bulk_insert_or_ignore('tags', tag_records)
                self.logger.debug(f"Stored {len(tag_records)} tags for event {event_id}")
        
        # Store event-tag relationships
        if event_tag_records:
            with self._lock:
                self.db_manager.bulk_insert_or_ignore('event_tags', event_tag_records)
                self.logger.debug(f"Stored {len(event_tag_records)} event-tag relationships")
    
    def store_market_tags(self, market_id: str, tags: List):
        """
        Store tags from a market response
        
        Args:
            market_id: The market ID
            tags: List of tags from market response
        """
        if not tags or not self.config.FETCH_TAGS:
            return
        
        tag_records = []
        market_tag_records = []
        
        for tag in tags:
            # Handle both string tags and tag objects
            if isinstance(tag, str):
                tag_id = tag
                tag_slug = tag
                tag_label = tag
                tag_data = {}
            elif isinstance(tag, dict):
                tag_id = tag.get('id', '')
                tag_slug = tag.get('slug', '')
                tag_label = tag.get('label', '')
                tag_data = tag
            else:
                continue
            
            if tag_id:
                # Prepare tag record
                tag_record = {
                    'id': tag_id,
                    'slug': tag_slug,
                    'label': tag_label,
                    'force_show': int(tag_data.get('forceShow', False)) if tag_data else 0,
                    'force_hide': int(tag_data.get('forceHide', False)) if tag_data else 0,
                    'is_carousel': int(tag_data.get('isCarousel', False)) if tag_data else 0,
                    'published_at': tag_data.get('publishedAt') if tag_data else None,
                    'created_by': tag_data.get('createdBy') if tag_data else None,
                    'updated_by': tag_data.get('updatedBy') if tag_data else None,
                    'created_at': tag_data.get('createdAt') if tag_data else None,
                    'updated_at': tag_data.get('updatedAt') if tag_data else None,
                    'fetched_at': datetime.now().isoformat()
                }
                tag_records.append(tag_record)
                
                # Prepare market-tag relationship
                market_tag_records.append({
                    'market_id': market_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                })
        
        # Store tags (ignore duplicates)
        if tag_records:
            with self._lock:
                self.db_manager.bulk_insert_or_ignore('tags', tag_records)
        
        # Store market-tag relationships
        if market_tag_records:
            with self._lock:
                self.db_manager.bulk_insert_or_ignore('market_tags', market_tag_records)
    
    def store_tag_relationships(self, tag_id: str, relationships: List[Dict]):
        """
        Store tag relationships
        
        Args:
            tag_id: The primary tag ID
            relationships: List of related tag relationships
        """
        if not relationships:
            return
        
        relationship_records = []
        
        for rel in relationships:
            record = {
                'tag_id': tag_id,
                'related_tag_id': rel.get('relatedTagId'),
                'relationship_type': rel.get('relationshipType', 'related'),
                'strength': rel.get('strength', 1.0),
                'created_at': datetime.now().isoformat()
            }
            relationship_records.append(record)
        
        if relationship_records:
            with self._lock:
                self.db_manager.bulk_insert_or_replace('tag_relationships', relationship_records)
                self.logger.debug(f"Stored {len(relationship_records)} tag relationships")
    
    def load_tags_only(self) -> Dict:
        """
        Load standalone tag data (for tags not attached to events/markets)
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️ Loading TAGS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            # Note: Tags are typically loaded from events and markets
            # This is a placeholder for standalone tag loading if needed
            result['count'] = 0
            result['success'] = True
            
            # Get current tag statistics
            stats = self.get_tag_statistics()
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Tag processing complete")
            self.logger.info(f"   Total tags in database: {stats.get('total_tags', 0)}")
            self.logger.info(f"   Tags with events: {stats.get('tags_with_events', 0)}")
            self.logger.info(f"   Tags with markets: {stats.get('tags_with_markets', 0)}")
            self.logger.info(f"⏱️ Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading tags: {e}")
            
        return result
    
    def delete_tags_only(self) -> Dict:
        """Delete tag data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️ Deleting TAGS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Get current count
            before_count = self.db_manager.get_table_count('tags')
            
            # Delete all related data
            self.db_manager.delete_records('event_tags', commit=False)
            self.db_manager.delete_records('market_tags', commit=False)
            self.db_manager.delete_records('series_tags', commit=False)
            self.db_manager.delete_records('tag_relationships', commit=False)
            deleted = self.db_manager.delete_records('tags', commit=True)
            
            result['deleted'] = before_count
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} tags and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting tags: {e}")
            
        return result
    
    def get_tag_statistics(self) -> Dict:
        """Get statistics about tags in the database"""
        stats = {}
        
        # Total tags
        total = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM tags")
        stats['total_tags'] = total['count'] if total else 0
        
        # Tags with events
        with_events = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count FROM event_tags
        """)
        stats['tags_with_events'] = with_events['count'] if with_events else 0
        
        # Tags with markets
        with_markets = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count FROM market_tags
        """)
        stats['tags_with_markets'] = with_markets['count'] if with_markets else 0
        
        # Tags with relationships
        with_relationships = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count FROM tag_relationships
        """)
        stats['tags_with_relationships'] = with_relationships['count'] if with_relationships else 0
        
        # Total relationships
        total_relationships = self.db_manager.fetch_one("""
            SELECT COUNT(*) as count FROM tag_relationships
        """)
        stats['total_relationships'] = total_relationships['count'] if total_relationships else 0
        
        return stats