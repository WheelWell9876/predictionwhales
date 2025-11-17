import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config



def _store_market_tags(self, market_id: str, tags: List[Dict]):
    """
    Store tags and relationships for a market
    Thread-safe when called with _db_lock
    """
    tag_records = []
    relationships = []
    
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
        
        relationship = {
            'market_id': market_id,
            'tag_id': tag.get('id')
        }
        relationships.append(relationship)
    
    # Bulk insert with lock
    if tag_records:
        with self._db_lock:
            self.bulk_insert_or_replace('tags', tag_records)
    if relationships:
        with self._db_lock:
            self.bulk_insert_or_replace('market_tags', relationships)
    
    self.logger.debug(f"Stored {len(tags)} tags for market {market_id}")