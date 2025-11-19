"""
Store Tags
Handles storage functionality for tags data
"""

from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreTagsManager(DatabaseManager):
    """Manager for storing tag data with thread-safe operations"""

    def __init__(self):
        super().__init__()
        from backend.config import Config
        self.config = Config
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()

    def _store_tags(self, tags: List[Dict], detailed: bool = False):
        """
        Store multiple tags in the database (thread-safe)
        """
        tag_records = []
        
        for tag in tags:
            record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            tag_records.append(record)
        
        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tags', tag_records)
                self.logger.debug(f"Stored {len(tag_records)} tags")

    def _store_tag_detailed(self, tag: Dict):
        """
        Store detailed tag information (thread-safe)
        """
        record = {
            'id': tag.get('id'),
            'label': tag.get('label'),
            'slug': tag.get('slug'),
            'force_show': tag.get('forceShow', False),
            'force_hide': tag.get('forceHide', False),
            'is_carousel': tag.get('isCarousel', False),
            'published_at': tag.get('publishedAt'),
            'created_by': tag.get('createdBy'),
            'updated_by': tag.get('updatedBy'),
            'created_at': tag.get('createdAt'),
            'updated_at': tag.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }
        
        with self._db_lock:
            self.insert_or_replace('tags', record)
            self.logger.debug(f"Stored detailed tag: {tag.get('id')}")

    def _store_tag_relationships(self, relationships: List[Dict]):
        """
        Store tag relationships (thread-safe)
        """
        if not relationships:
            return
        
        relationship_records = []
        
        for relationship in relationships:
            record = {
                'tag_id': relationship.get('tagId'),
                'related_tag_id': relationship.get('relatedTagId'),
                'relationship_type': relationship.get('relationshipType'),
                'strength': relationship.get('strength', 1.0),
                'created_at': datetime.now().isoformat()
            }
            relationship_records.append(record)
        
        if relationship_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tag_relationships', relationship_records)
                self.logger.debug(f"Stored {len(relationship_records)} tag relationships")

    def _store_event_tags_basic(self, event_id: str, tags: List):
        """
        Store basic tag information from event response
        """
        if not tags:
            return
        
        tag_records = []
        event_tag_records = []
        
        for tag in tags:
            if isinstance(tag, dict):
                tag_id = tag.get('id')
                tag_slug = tag.get('slug')
                tag_label = tag.get('label')
            else:
                # If tag is just a string
                tag_id = tag
                tag_slug = tag
                tag_label = tag
            
            if tag_id:
                # Store tag
                tag_record = {
                    'id': tag_id,
                    'label': tag_label,
                    'slug': tag_slug,
                    'force_show': tag.get('forceShow', False) if isinstance(tag, dict) else False,
                    'is_carousel': tag.get('isCarousel', False) if isinstance(tag, dict) else False,
                    'published_at': tag.get('publishedAt') if isinstance(tag, dict) else None,
                    'created_at': tag.get('createdAt') if isinstance(tag, dict) else datetime.now().isoformat(),
                    'updated_at': tag.get('updatedAt') if isinstance(tag, dict) else datetime.now().isoformat()
                }
                tag_records.append(tag_record)
                
                # Store event-tag relationship
                event_tag_records.append({
                    'event_id': event_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                })
        
        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_ignore('tags', tag_records)
        
        if event_tag_records:
            with self._db_lock:
                self.bulk_insert_or_ignore('event_tags', event_tag_records)

    def _fetch_and_store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store detailed tag information and relationships
        """
        if not tags:
            return
        
        tag_records = []
        event_tag_records = []
        
        for tag in tags:
            tag_record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            tag_records.append(tag_record)
            
            # Store event-tag relationship
            event_tag_records.append({
                'event_id': event_id,
                'tag_id': tag.get('id'),
                'tag_slug': tag.get('slug')
            })
        
        # Bulk insert tags
        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tags', tag_records)
        
        # Store event-tag relationships
        if event_tag_records:
            with self._db_lock:
                self.bulk_insert_or_replace('event_tags', event_tag_records)
        
        self.logger.debug(f"Stored {len(tags)} tags for event {event_id}")

    def _store_market_tags(self, market_id: str, tags: List[Dict]):
        """
        Store market tags and relationships (thread-safe)
        """
        if not tags:
            return
        
        tag_records = []
        market_tag_records = []
        
        for tag in tags:
            if isinstance(tag, dict):
                tag_id = tag.get('id')
                tag_slug = tag.get('slug')
                tag_label = tag.get('label')
            else:
                # If tag is just a string
                tag_id = tag
                tag_slug = tag
                tag_label = tag
            
            if tag_id:
                # Store tag
                tag_record = {
                    'id': tag_id,
                    'label': tag_label,
                    'slug': tag_slug,
                    'created_at': datetime.now().isoformat(),
                    'updated_at': datetime.now().isoformat()
                }
                tag_records.append(tag_record)
                
                # Store market-tag relationship
                market_tag_records.append({
                    'market_id': market_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                })
        
        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_ignore('tags', tag_records)
        
        if market_tag_records:
            with self._db_lock:
                self.bulk_insert_or_ignore('market_tags', market_tag_records)
        
        self.logger.debug(f"Stored {len(tags)} tags for market {market_id}")