"""
Batch tags
Handles batch fetching for the tags
"""

import requests
import json
from datetime import datetime
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_tags import StoreTagsManager

class BatchTagsManager(DatabaseManager):
    """Manager for batch tag fetching"""

    def __init__(self):
        super().__init__()
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreTagsManager()
        
        # Set max workers
        self.max_workers = min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
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
            self.store_manager._store_tags(tags)

            self.logger.info(f"Fetched and stored {len(tags)} tags")

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")

        return all_tags

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
                self.store_manager._store_tag_relationships(relationships)

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
            self.store_manager._fetch_and_store_event_tags(event_id, tags)
            
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
                self.store_manager._store_market_tags(market_id, tags)
            
            return tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for market {market_id}: {e}")
            return []