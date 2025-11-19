"""
Store Comments
Handles storage functionality for comments data
"""

from datetime import datetime
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreCommentsManager(DatabaseManager):
    """Manager for storing comment data with thread-safe operations"""

    def __init__(self):
        super().__init__()
        from backend.config import Config
        self.config = Config
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()

    def _store_comments(self, comments: List[Dict], event_id: str = None, market_id: str = None):
        """
        Store comments in database (thread-safe)
        
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
                with self._db_lock:
                    self.insert_or_ignore('users', user_record)

        if comment_records:
            with self._db_lock:
                self.bulk_insert_or_replace('comments', comment_records)
                self.logger.debug(f"Stored {len(comment_records)} comments")

    def _store_comment_reactions(self, comment_id: str, reactions: List[Dict]):
        """
        Store comment reactions in database (thread-safe)
        
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
                with self._db_lock:
                    self.insert_or_ignore('users', user_record)

        if reaction_records:
            with self._db_lock:
                self.bulk_insert_or_replace('comment_reactions', reaction_records)
                self.logger.debug(f"Stored {len(reaction_records)} reactions for comment {comment_id}")

    def _store_user_comments(self, comments: List[Dict]):
        """
        Store user comments (thread-safe)
        """
        comment_records = []
        
        for comment in comments:
            record = {
                'id': comment.get('id'),
                'event_id': comment.get('eventID'),
                'market_id': comment.get('marketID'),
                'proxy_wallet': comment.get('userAddress'),
                'username': comment.get('username'),
                'profile_image': comment.get('profileImage'),
                'content': comment.get('content') or comment.get('body'),
                'parent_comment_id': comment.get('parentCommentID'),
                'created_at': comment.get('createdAt'),
                'updated_at': comment.get('updatedAt'),
                'likes_count': comment.get('likesCount', 0) or comment.get('reactionCount', 0),
                'replies_count': comment.get('repliesCount', 0)
            }
            comment_records.append(record)
        
        if comment_records:
            with self._db_lock:
                self.bulk_insert_or_replace('comments', comment_records)
                self.logger.debug(f"Stored {len(comment_records)} user comments")