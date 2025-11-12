"""
Tags Manager for Polymarket Terminal
Handles fetching, processing, and storing tag data and relationships with multithreading support
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database_manager import DatabaseManager
from .config import Config

class TagsManager(DatabaseManager):
    """Manager for tag-related operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to min of 10 or available CPU cores * 2)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
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

    def fetch_tag_by_id(self, tag_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}"
            params = {"include_template": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            tag = response.json()

            # Store detailed tag
            self._store_tag_detailed(tag)

            return tag

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tag {tag_id}: {e}")
            return None

    def fetch_tag_by_id_parallel(self, tag_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific tag with parallel sub-requests
        Fetches tag details, relationships, and related tags concurrently
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}"
            params = {"include_template": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            tag = response.json()

            # Parallel execution of sub-tasks
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                
                # Store tag details
                futures.append(executor.submit(self._store_tag_detailed, tag))
                
                # Fetch relationships
                futures.append(executor.submit(self.fetch_tag_relationships, tag_id))
                
                # Fetch related tags details
                futures.append(executor.submit(self.fetch_related_tags_details, tag_id))
                
                # Wait for all to complete and collect results
                relationships_count = 0
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        # Track relationships count
                        if isinstance(result, list):
                            relationships_count += len(result)
                    except Exception as e:
                        self.logger.error(f"Error in parallel tag fetch subtask: {e}")
                
                # Update relationships counter
                with self._progress_lock:
                    self._relationships_counter += relationships_count

            return tag

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tag {tag_id}: {e}")
            return None

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

    def fetch_related_tags_details(self, tag_id: str) -> List[Dict]:
        """
        Fetch full details of tags related to a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}/related-tags/tags"
            params = {"status": "all", "omit_empty": "true"}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()

            related_tags = response.json()

            if related_tags:
                self._store_tags(related_tags, detailed=True)

            return related_tags

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching related tags details for tag {tag_id}: {e}")
            return []

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

    def process_all_tags_detailed(self, use_parallel: bool = True):
        """
        Process all tags to fetch detailed information and relationships with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests (relationships, related tags)
        """
        # Get all tag IDs from database
        tags = self.fetch_all("SELECT id FROM tags ORDER BY id")

        self.logger.info(f"Processing {len(tags)} tags for detailed information using {self.max_workers} threads...")

        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._relationships_counter = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Choose which fetch method to use
            fetch_method = self.fetch_tag_by_id_parallel if use_parallel else self._fetch_tag_detailed_sequential
            
            # Submit all tasks
            future_to_tag = {
                executor.submit(self._process_tag_detailed, tag, fetch_method, len(tags)): tag 
                for tag in tags
            }
            
            # Process completed tasks
            for future in as_completed(future_to_tag):
                tag = future_to_tag[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing tag {tag['id']}: {e}")

        self.logger.info(f"Tag processing complete. Processed: {self._progress_counter}, Errors: {self._error_counter}")
        self.logger.info(f"Total relationships stored: {self._relationships_counter}")

    def _fetch_tag_detailed_sequential(self, tag_id: str) -> Optional[Dict]:
        """
        Sequential version of tag fetching (non-parallel sub-requests)
        Used when use_parallel=False
        """
        # Fetch detailed tag information
        tag = self.fetch_tag_by_id(tag_id)
        
        if not tag:
            return None

        # Fetch tag relationships
        relationships = self.fetch_tag_relationships(tag_id)
        with self._progress_lock:
            self._relationships_counter += len(relationships)

        # Fetch related tags details
        self.fetch_related_tags_details(tag_id)

        return tag

    def _process_tag_detailed(self, tag: Dict, fetch_method, total_tags: int):
        """
        Helper method to process a single tag
        Thread-safe wrapper for parallel execution
        """
        try:
            fetch_method(tag['id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 50 == 0:
                    self.logger.info(f"Processed {self._progress_counter}/{total_tags} tags")
                    self.logger.info(f"Total relationships found: {self._relationships_counter}")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

    def batch_fetch_tags(self, tag_ids: List[str], use_parallel: bool = True) -> List[Dict]:
        """
        Fetch multiple tags in parallel
        
        Args:
            tag_ids: List of tag IDs to fetch
            use_parallel: If True, uses parallel fetching for sub-requests
            
        Returns:
            List of tag dictionaries
        """
        self.logger.info(f"Batch fetching {len(tag_ids)} tags using {self.max_workers} threads...")
        
        tags_results = []
        fetch_method = self.fetch_tag_by_id_parallel if use_parallel else self.fetch_tag_by_id
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(fetch_method, tag_id): tag_id 
                for tag_id in tag_ids
            }
            
            for future in as_completed(future_to_id):
                tag_id = future_to_id[future]
                try:
                    tag = future.result()
                    if tag:
                        tags_results.append(tag)
                except Exception as e:
                    self.logger.error(f"Error fetching tag {tag_id}: {e}")
        
        return tags_results

    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for tag updates with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        if not self.config.FETCH_TAGS:
            self.logger.info("Tag fetching disabled")
            return 0

        self.logger.info("Starting daily tag scan with multithreading...")

        # Fetch all tags
        all_tags = self.fetch_all_tags()

        # Process detailed information and relationships (parallelized)
        self.process_all_tags_detailed(use_parallel=use_parallel)

        # Get statistics
        stats = self._get_tag_statistics()

        self.logger.info(f"Daily tag scan complete. Total tags: {len(all_tags)}")
        self.logger.info(f"Tag statistics: {stats}")

        return len(all_tags)

    def _get_tag_statistics(self) -> Dict:
        """
        Get statistics about tags in the database
        """
        stats = {}

        # Total tags
        stats['total_tags'] = self.get_table_count('tags')

        # Tags with relationships
        result = self.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count
            FROM tag_relationships
        """)
        stats['tags_with_relationships'] = result['count'] if result else 0

        # Total relationships
        stats['total_relationships'] = self.get_table_count('tag_relationships')

        # Most used tags (in events)
        result = self.fetch_all("""
            SELECT t.label, 
                   LENGTH(et.event_ids) - LENGTH(REPLACE(et.event_ids, ',', '')) + 1 as usage_count
            FROM tags t
            JOIN event_tags et ON t.id = et.tag_id
            ORDER BY usage_count DESC
            LIMIT 10
        """)
        stats['most_used_tags'] = result
        
        return stats