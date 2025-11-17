"""
Batch tags
Handles batch fetching for the tags
"""



from datetime import datetime
import json
from typing import Dict, List

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class BatchTagsManager(DatabaseManager):
    """Manager for tag-related operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._relationships_counter = 0

    def fetch_all_tags(self, limit: int = 1000) -> List[Dict]:
            """
            Fetch all tags from the API
            Used for initial data load and daily scans
            """
            all_tags = []

            self.logger.info("Starting to fetch all tags...")

            try:
                url = f"{self.base_url}/tags"
                params = {"limit": limit}

                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response.raise_for_status()

                tags = response.json()
                all_tags.extend(tags)

                # Store tags
                self._store_tags(tags)

                self.logger.info(f"Fetched and stored {len(tags)} tags")

            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching tags: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")

            return all_tags




    
    def _store_event_tags_basic(self, event_id: str, tags: List[Dict]):
        """
        Store basic tag information from event response
        """
        for tag in tags:
            tag_id = tag.get('id')
            
            # Store tag
            tag_record = {
                'id': tag_id,
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            }
            self.insert_or_ignore('tags', tag_record)
            
            # Store event-tag relationship
            conn = self.get_persistent_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT event_ids FROM event_tags WHERE tag_id = ?", (tag_id,))
            result = cursor.fetchone()
            
            if result:
                event_ids = json.loads(result[0])
                if event_id not in event_ids:
                    event_ids.append(event_id)
                    cursor.execute(
                        "UPDATE event_tags SET event_ids = ? WHERE tag_id = ?",
                        (json.dumps(event_ids), tag_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO event_tags (tag_id, event_ids) VALUES (?, ?)",
                    (tag_id, json.dumps([event_id]))
                )
            
            conn.commit()
    
    def _fetch_and_store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store detailed tag information and relationships
        """
        tag_records = []
        
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
        
        # Bulk insert tags
        if tag_records:
            self.bulk_insert_or_replace('tags', tag_records)
        
        # Store event-tag relationships
        for tag in tags:
            tag_id = tag.get('id')
            
            conn = self.get_persistent_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT event_ids FROM event_tags WHERE tag_id = ?", (tag_id,))
            result = cursor.fetchone()
            
            if result:
                event_ids = json.loads(result[0])
                if event_id not in event_ids:
                    event_ids.append(event_id)
                    cursor.execute(
                        "UPDATE event_tags SET event_ids = ? WHERE tag_id = ?",
                        (json.dumps(event_ids), tag_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO event_tags (tag_id, event_ids) VALUES (?, ?)",
                    (tag_id, json.dumps([event_id]))
                )
            
            conn.commit()
        
        self.logger.debug(f"Stored {len(tags)} tags for event {event_id}")


    def fetch_tag_relationships(self, tag_id: str) -> List[Dict]:
        """
        Fetch relationships for a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}/related-tags"
            params = {"status": "all", "omit_empty": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            relationships = response.json()

            if relationships:
                self._store_tag_relationships(relationships)

            return relationships

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching relationships for tag {tag_id}: {e}")
            return []


    def fetch_event_tags(self, event_id: str) -> List[Dict]:
        """
        Fetch tags for a specific event
        """
        try:
            url = f"{self.base_url}/events/{event_id}/tags"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            
            # Store tags and relationships
            self._fetch_and_store_event_tags(event_id, tags)
            
            return tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for event {event_id}: {e}")
            return []
        

    def fetch_market_tags(self, market_id: str) -> List[Dict]:
        """
        Fetch tags for a specific market
        """
        try:
            url = f"{self.base_url}/markets/{market_id}/tags"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            
            # Store tags and relationships
            if tags:
                self._store_market_tags(market_id, tags)
            
            return tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for market {market_id}: {e}")
            return []