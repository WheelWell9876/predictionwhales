"""
Comments Manager for Polymarket Terminal
Handles fetching, processing, and storing comments and reactions for events and markets
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config

class CommentsManager(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
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


    def _fetch_comments(self, parent_entity_type: str, parent_entity_id: str, limit: int = 15) -> List[Dict]:
        """
        Fetch comments for a specific entity
        
        Args:
            parent_entity_type: 'Event', 'market', or 'Series'
            parent_entity_id: ID of the parent entity
            limit: Number of comments to fetch
            
        Returns:
            List of comment dictionaries
        """
        try:
            url = f"{self.base_url}/comments"
            params = {
                "parent_entity_type": parent_entity_type,
                "parent_entity_id": parent_entity_id,
                "limit": limit,
                "offset": 0,
                "order": "createdAt",
                "ascending": "false",
                "get_positions": "true"
            }
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                return response.json() or []
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching comments for {parent_entity_type} {parent_entity_id}: {e}")
            return []

    def _fetch_comment_reactions(self, comment_id: str) -> List[Dict]:
        """
        Fetch reactions for a specific comment
        
        Args:
            comment_id: ID of the comment
            
        Returns:
            List of reaction dictionaries
        """
        try:
            url = f"{self.base_url}/comments/{comment_id}/reactions"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                return response.json() or []
            
            return []
            
        except Exception as e:
            return []
