from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreTags(DatabaseManager):
    """Manager for storing tags with multithreading support"""

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


    def _store_tags(self, tags: List[Dict], detailed: bool = False):
            """
            Store multiple tags in the database
            Thread-safe when called with _db_lock
            """
            tag_records = []

            for tag in tags:
                record = self._prepare_tag_record(tag, detailed)
                tag_records.append(record)

            # Bulk insert tags with lock
            if tag_records:
                with self._db_lock:
                    self.bulk_insert_or_replace('tags', tag_records)
                self.logger.debug(f"Stored {len(tag_records)} tags")

    def _store_tag_detailed(self, tag: Dict):
        """
        Store detailed tag information
        Thread-safe when called with _db_lock
        """
        record = self._prepare_tag_record(tag, detailed=True)
        with self._db_lock:
            self.insert_or_replace('tags', record)
        self.logger.debug(f"Stored detailed tag: {tag.get('id')}")

    def _prepare_tag_record(self, tag: Dict, detailed: bool = False) -> Dict:
        """
        Prepare a tag record for database insertion
        """
        record = {
            'id': tag.get('id'),
            'label': tag.get('label'),
            'slug': tag.get('slug'),
            'force_show': tag.get('forceShow', False),
            'fetched_at': datetime.now().isoformat()
        }

        if detailed:
            record.update({
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            })

        return record

    def _store_tag_relationships(self, relationships: List[Dict]):
        """
        Store tag relationships in the database
        Thread-safe when called with _db_lock
        """
        relationship_records = []

        for rel in relationships:
            record = {
                'id': rel.get('id'),
                'tag_id': rel.get('tagID'),
                'related_tag_id': rel.get('relatedTagID'),
                'rank': rel.get('rank'),
                'created_at': datetime.now().isoformat()
            }
            relationship_records.append(record)

        # Bulk insert relationships with lock
        if relationship_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tag_relationships', relationship_records)
            self.logger.debug(f"Stored {len(relationship_records)} tag relationships")


    def _store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store tags for an event in a series
        Thread-safe when called with _db_lock
        """
        tag_records = []

        for tag in tags:
            tag_record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'is_carousel': tag.get('isCarousel', False),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            }
            tag_records.append(tag_record)

        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tags', tag_records)

        # Store event-tag relationships
        with self._db_lock:
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
    
    def _store_collection_tags(self, collection_id: str, tags: List[Dict]):
        """
        Store tags for a collection
        Thread-safe when called with _db_lock
        """
        relationships = []
        
        for tag in tags:
            # Store tag if it has complete data
            if isinstance(tag, dict) and 'id' in tag:
                tag_record = {
                    'id': tag.get('id'),
                    'label': tag.get('label'),
                    'slug': tag.get('slug'),
                    'created_at': tag.get('createdAt'),
                    'updated_at': tag.get('updatedAt')
                }
                with self._db_lock:
                    self.insert_or_ignore('tags', tag_record)
                
                relationship = {
                    'collection_id': collection_id,
                    'tag_id': tag.get('id')
                }
                relationships.append(relationship)
        
        if relationships:
            with self._db_lock:
                self.bulk_insert_or_replace('collection_tags', relationships)