"""
ID tags
Handles individual fetching for the tags
"""

from typing import Dict, List, Optional
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class IdTagsManager(DatabaseManager):
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