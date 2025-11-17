from datetime import time, datetime
from threading import Lock
from typing import Dict, List

from backend.database.database_manager import DatabaseManager

class StoreComments(DatabaseManager):
    """Manager for comments and reactions operations with multithreading support"""

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
                    self.logger.info(
                        f"  Progress: {self._progress_counter}/{total_markets} markets, {self._comments_counter} comments")

            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)

        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

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




    
    def _store_user_comments(self, comments: List[Dict]):
        """Store user comments (thread-safe when called with _db_lock)"""
        comment_records = []
        
        for comment in comments:
            record = {
                'id': comment.get('id'),
                'event_id': comment.get('eventID'),
                'market_id': comment.get('marketID'),
                'proxy_wallet': comment.get('userAddress'),
                'username': comment.get('username'),
                'profile_image': comment.get('profileImage'),
                'content': comment.get('content'),
                'parent_comment_id': comment.get('parentCommentID'),
                'created_at': comment.get('createdAt'),
                'updated_at': comment.get('updatedAt'),
                'likes_count': comment.get('likesCount', 0),
                'replies_count': comment.get('repliesCount', 0)
            }
            comment_records.append(record)
        
        if comment_records:
            self.bulk_insert_or_replace('comments', comment_records)
    
    def _store_comment_reactions(self, comment_id: str, reactions: List[Dict]):
        """Store comment reactions (thread-safe when called with _db_lock)"""
        reaction_records = []
        
        for reaction in reactions:
            record = {
                'comment_id': comment_id,
                'proxy_wallet': reaction.get('userAddress'),
                'reaction_type': reaction.get('type', 'LIKE'),
                'created_at': reaction.get('createdAt') or datetime.now().isoformat()
            }
            reaction_records.append(record)
        
        if reaction_records:
            self.bulk_insert_or_replace('comment_reactions', reaction_records)