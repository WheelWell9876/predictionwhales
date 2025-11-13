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
from .database_manager import DatabaseManager
from .config import Config

class CommentsManager(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
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
                # Store comments and fetch reactions
                with self._db_lock:
                    self._store_comments(comments, event_id=event['id'])
                
                with self._progress_lock:
                    self._comments_counter += len(comments)
                
                # Fetch reactions for each comment
                for comment in comments:
                    reactions = self._fetch_comment_reactions(comment['id'])
                    if reactions:
                        with self._db_lock:
                            self._store_comment_reactions(comment['id'], reactions)
                        with self._progress_lock:
                            self._reactions_counter += len(reactions)
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 50 == 0 or self._progress_counter == total_events:
                    self.logger.info(f"  Progress: {self._progress_counter}/{total_events} events, {self._comments_counter} comments")
            
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
                # Store comments and fetch reactions
                with self._db_lock:
                    self._store_comments(comments, market_id=market['id'])
                
                with self._progress_lock:
                    self._comments_counter += len(comments)
                
                # Fetch reactions for each comment
                for comment in comments:
                    reactions = self._fetch_comment_reactions(comment['id'])
                    if reactions:
                        with self._db_lock:
                            self._store_comment_reactions(comment['id'], reactions)
                        with self._progress_lock:
                            self._reactions_counter += len(reactions)
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 100 == 0 or self._progress_counter == total_markets:
                    self.logger.info(f"  Progress: {self._progress_counter}/{total_markets} markets, {self._comments_counter} comments")
            
            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

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

    def _store_comments(self, comments: List[Dict], event_id: str = None, market_id: str = None):
        """
        Store comments in database (thread-safe when called with _db_lock)
        
        Args:
            comments: List of comment dictionaries
            event_id: Event ID if comments are for an event
            market_id: Market ID if comments are for a market
        """
        comment_records = []
        
        for comment in comments:
            # Extract profile data
            profile = comment.get('profile', {})
            
            record = {
                'id': comment.get('id'),
                'event_id': event_id,
                'market_id': market_id,
                'proxy_wallet': comment.get('userAddress') or profile.get('proxyWallet'),
                'username': profile.get('name') or profile.get('pseudonym'),
                'profile_image': profile.get('profileImage'),
                'content': comment.get('body'),
                'parent_comment_id': comment.get('parentCommentID'),
                'created_at': comment.get('createdAt'),
                'updated_at': comment.get('updatedAt'),
                'likes_count': comment.get('reactionCount', 0),
                'replies_count': 0  # Can be computed later if needed
            }
            comment_records.append(record)
            
            # Store user profile if we have it
            if profile and profile.get('proxyWallet'):
                user_record = {
                    'proxy_wallet': profile.get('proxyWallet'),
                    'username': profile.get('name') or profile.get('pseudonym'),
                    'bio': profile.get('bio'),
                    'profile_image': profile.get('profileImage'),
                    'last_updated': datetime.now().isoformat()
                }
                self.insert_or_ignore('users', user_record)
        
        if comment_records:
            self.bulk_insert_or_replace('comments', comment_records)
            self.logger.debug(f"Stored {len(comment_records)} comments")

    def _store_comment_reactions(self, comment_id: str, reactions: List[Dict]):
        """
        Store comment reactions in database (thread-safe when called with _db_lock)
        
        Args:
            comment_id: ID of the comment
            reactions: List of reaction dictionaries
        """
        reaction_records = []
        
        for reaction in reactions:
            # Extract profile data
            profile = reaction.get('profile', {})
            
            record = {
                'comment_id': comment_id,
                'proxy_wallet': reaction.get('userAddress') or profile.get('proxyWallet'),
                'reaction_type': reaction.get('reactionType', 'LIKE'),
                'created_at': reaction.get('createdAt') or datetime.now().isoformat()
            }
            reaction_records.append(record)
            
            # Store user profile if we have it
            if profile and profile.get('proxyWallet'):
                user_record = {
                    'proxy_wallet': profile.get('proxyWallet'),
                    'username': profile.get('name') or profile.get('pseudonym'),
                    'profile_image': profile.get('profileImage'),
                    'last_updated': datetime.now().isoformat()
                }
                self.insert_or_ignore('users', user_record)
        
        if reaction_records:
            self.bulk_insert_or_replace('comment_reactions', reaction_records)
            self.logger.debug(f"Stored {len(reaction_records)} reactions for comment {comment_id}")

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