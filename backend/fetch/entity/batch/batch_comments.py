"""
Batch comments
Handles batch fetching for the comments of markets and events
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_comments import StoreCommentsManager

class BatchCommentsManager(DatabaseManager):
    """Manager for batch comment fetching with multithreading support"""

    def __init__(self):
        super().__init__()
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreCommentsManager()
        
        # Set max workers
        self.max_workers = min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0

    def fetch_comments_for_all_events(self, limit_per_event: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active events with multithreading
        
        Args:
            limit_per_event: Number of comments to fetch per event (default: 15)
            
        Returns:
            Dictionary with statistics
        """
        self.logger.info(f"💬 Fetching top {limit_per_event} comments for all active events...")
        
        # Get all active events
        events = self.fetch_all("""
            SELECT id, title FROM events 
            WHERE active = 1 
            ORDER BY volume DESC
        """)
        
        self.logger.info(f"Processing {len(events)} events using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_event = {
                executor.submit(self._fetch_and_store_event_comments, event, limit_per_event, len(events)): event 
                for event in events
            }
            
            # Process completed tasks
            for future in as_completed(future_to_event):
                event = future_to_event[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing event {event['id']}: {e}")
        
        self.logger.info(f"✅ Comments fetch complete!")
        self.logger.info(f"   Events processed: {self._progress_counter}")
        self.logger.info(f"   Comments fetched: {self._comments_counter}")
        self.logger.info(f"   Reactions fetched: {self._reactions_counter}")
        self.logger.info(f"   Errors: {self._error_counter}")
        
        return {
            'events_processed': self._progress_counter,
            'comments_fetched': self._comments_counter,
            'reactions_fetched': self._reactions_counter,
            'errors': self._error_counter
        }

    def fetch_comments_for_all_markets(self, limit_per_market: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active markets with multithreading
        
        Args:
            limit_per_market: Number of comments to fetch per market (default: 15)
            
        Returns:
            Dictionary with statistics
        """
        self.logger.info(f"💬 Fetching top {limit_per_market} comments for all active markets...")
        
        # Get all active markets
        markets = self.fetch_all("""
            SELECT id, question FROM markets 
            WHERE active = 1 
            ORDER BY volume DESC
        """)
        
        self.logger.info(f"Processing {len(markets)} markets using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_market = {
                executor.submit(self._fetch_and_store_market_comments, market, limit_per_market, len(markets)): market 
                for market in markets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing market {market['id']}: {e}")
        
        self.logger.info(f"✅ Comments fetch complete!")
        self.logger.info(f"   Markets processed: {self._progress_counter}")
        self.logger.info(f"   Comments fetched: {self._comments_counter}")
        self.logger.info(f"   Reactions fetched: {self._reactions_counter}")
        self.logger.info(f"   Errors: {self._error_counter}")
        
        return {
            'markets_processed': self._progress_counter,
            'comments_fetched': self._comments_counter,
            'reactions_fetched': self._reactions_counter,
            'errors': self._error_counter
        }

    def _fetch_and_store_event_comments(self, event: Dict, limit: int, total_events: int):
        """
        Thread-safe wrapper for fetching and storing event comments
        """
        try:
            comments = self._fetch_comments(
                parent_entity_type='Event',
                parent_entity_id=event['id'],
                limit=limit
            )

            if comments:
                # Store comments
                with self._lock:
                    self.store_manager._store_comments(comments, event_id=event['id'])

                with self._progress_lock:
                    self._comments_counter += len(comments)

                # Fetch reactions for each comment
                for comment in comments:
                    reactions = self._fetch_comment_reactions(comment['id'])
                    if reactions:
                        with self._lock:
                            self.store_manager._store_comment_reactions(comment['id'], reactions)
                        with self._progress_lock:
                            self._reactions_counter += len(reactions)

            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 50 == 0 or self._progress_counter == total_events:
                    self.logger.info(
                        f"  Progress: {self._progress_counter}/{total_events} events, {self._comments_counter} comments")

            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)

        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

    def _fetch_and_store_market_comments(self, market: Dict, limit: int, total_markets: int):
        """
        Thread-safe wrapper for fetching and storing market comments
        """
        try:
            comments = self._fetch_comments(
                parent_entity_type='market',
                parent_entity_id=market['id'],
                limit=limit
            )

            if comments:
                # Store comments
                with self._lock:
                    self.store_manager._store_comments(comments, market_id=market['id'])

                with self._progress_lock:
                    self._comments_counter += len(comments)

                # Fetch reactions for each comment
                for comment in comments:
                    reactions = self._fetch_comment_reactions(comment['id'])
                    if reactions:
                        with self._lock:
                            self.store_manager._store_comment_reactions(comment['id'], reactions)
                        with self._progress_lock:
                            self._reactions_counter += len(reactions)

            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 100 == 0 or self._progress_counter == total_markets:
                    self.logger.info(
                        f"  Progress: {self._progress_counter}/{total_markets} markets, {self._comments_counter} comments")

            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)

        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

    def _fetch_comments(self, parent_entity_type: str, parent_entity_id: str, limit: int) -> List[Dict]:
        """
        Fetch comments for a specific entity (event or market)
        """
        try:
            url = f"{self.base_url}/comments"
            params = {
                "parentEntityType": parent_entity_type,
                "parentEntityId": parent_entity_id,
                "limit": limit,
                "order": "newest"
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
            self.logger.error(f"Error fetching reactions for comment {comment_id}: {e}")
            return []