"""
ID comments
Handles ID fetching for the comments of markets events
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager
from ....config import Config

class IdCommentsManager(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
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



    def fetch_comments_for_specific_entities(self, events: List[str] = None, markets: List[str] = None, limit: int = 15) -> Dict[str, int]:
        """
        Fetch comments for specific events and/or markets
        
        Args:
            events: List of event IDs
            markets: List of market IDs
            limit: Number of comments per entity
            
        Returns:
            Statistics dictionary
        """
        self._comments_counter = 0
        self._reactions_counter = 0
        
        if events:
            for event_id in events:
                comments = self._fetch_comments('Event', event_id, limit)
                if comments:
                    with self._db_lock:
                        self._store_comments(comments, event_id=event_id)
                    self._comments_counter += len(comments)
                    
                    for comment in comments:
                        reactions = self._fetch_comment_reactions(comment['id'])
                        if reactions:
                            with self._db_lock:
                                self._store_comment_reactions(comment['id'], reactions)
                            self._reactions_counter += len(reactions)
        
        if markets:
            for market_id in markets:
                comments = self._fetch_comments('market', market_id, limit)
                if comments:
                    with self._db_lock:
                        self._store_comments(comments, market_id=market_id)
                    self._comments_counter += len(comments)
                    
                    for comment in comments:
                        reactions = self._fetch_comment_reactions(comment['id'])
                        if reactions:
                            with self._db_lock:
                                self._store_comment_reactions(comment['id'], reactions)
                            self._reactions_counter += len(reactions)
        
        return {
            'comments_fetched': self._comments_counter,
            'reactions_fetched': self._reactions_counter
        }
    



    def _fetch_user_comments(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user's comments"""
        try:
            url = f"{self.base_url}/comments"
            params = {"userAddress": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                comments = response.json() or []
                
                if comments:
                    with self._db_lock:
                        self._store_user_comments(comments)
                    
                    # Fetch reactions for each comment (in parallel within this method)
                    self._fetch_comments_reactions_parallel(comments)
                
                return comments
            
            return []
            
        except Exception as e:
            return []
        



    def _fetch_comments_reactions_parallel(self, comments: List[Dict]):
        """Fetch reactions for multiple comments in parallel"""
        if not comments:
            return
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_comment_reactions, comment.get('id')): comment 
                for comment in comments if comment.get('id')
            }
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error fetching comment reactions: {e}")




    def _fetch_comment_reactions(self, comment_id: str) -> List[Dict]:
        """Fetch reactions for a specific comment"""
        try:
            url = f"{self.base_url}/comments/{comment_id}/reactions"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                reactions = response.json() or []
                
                if reactions:
                    with self._db_lock:
                        self._store_comment_reactions(comment_id, reactions)
                
                return reactions
            
            return []
            
        except Exception as e:
            return []