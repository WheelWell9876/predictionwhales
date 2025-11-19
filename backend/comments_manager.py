"""
Comments Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing comment data with concurrent requests
"""

import requests
import time
import sqlite3
import gc
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.fetch.entity.batch.batch_comments import BatchCommentsManager
from backend.fetch.entity.id.id_comments import IdCommentsManager
from backend.database.entity.store_comments import StoreCommentsManager

class CommentsManager:
    """Manager for comment-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchCommentsManager()
        self.id_manager = IdCommentsManager()
        self.store_manager = StoreCommentsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()

    def fetch_comments_for_all_events(self, limit_per_event: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active events with multithreading
        
        Args:
            limit_per_event: Number of comments to fetch per event
        """
        return self.batch_manager.fetch_comments_for_all_events(limit_per_event)

    def fetch_comments_for_all_markets(self, limit_per_market: int = 15) -> Dict[str, int]:
        """
        Fetch top comments for all active markets with multithreading
        
        Args:
            limit_per_market: Number of comments to fetch per market
        """
        return self.batch_manager.fetch_comments_for_all_markets(limit_per_market)

    def fetch_comments_for_specific_entities(self, events: List[str] = None, markets: List[str] = None, limit: int = 15) -> Dict[str, int]:
        """
        Fetch comments for specific events and/or markets
        
        Args:
            events: List of event IDs
            markets: List of market IDs
            limit: Number of comments per entity
        """
        return self.id_manager.fetch_comments_for_specific_entities(events, markets, limit)

    def fetch_user_comments(self, proxy_wallet: str) -> List[Dict]:
        """
        Fetch comments for a specific user
        
        Args:
            proxy_wallet: User's proxy wallet address
        """
        return self.id_manager.fetch_user_comments(proxy_wallet)

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all comment manager database connections...")
        
        # Close connections from all sub-managers
        managers = [
            self.db_manager,
            self.batch_manager,
            self.id_manager,
            self.store_manager
        ]
        
        for manager in managers:
            try:
                if hasattr(manager, 'close_connection'):
                    manager.close_connection()
            except:
                pass
        
        # Force garbage collection
        gc.collect()
        
        # Small delay to ensure connections are closed
        time.sleep(0.5)

    def delete_comments_only(self) -> Dict:
        """
        Delete comments data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting COMMENTS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Create a fresh database connection for deletion
            conn = sqlite3.connect(
                self.db_manager.db_path,
                timeout=30.0,
                isolation_level='EXCLUSIVE'
            )
            
            try:
                cursor = conn.cursor()
                
                # Enable WAL mode
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                
                # Get current count
                cursor.execute("SELECT COUNT(*) FROM comments")
                before_count = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all related data
                tables_to_clear = [
                    'comment_reactions',
                    'comments'
                ]
                
                for table in tables_to_clear:
                    cursor.execute(f"DELETE FROM {table}")
                    self.logger.info(f"  Cleared table: {table}")
                
                # Commit the transaction
                conn.commit()
                
                result['deleted'] = before_count
                
            finally:
                conn.close()
            
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} comments and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting comments: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_comments_only(self, limit_per_event: int = 15, events_only: bool = True) -> Dict:
        """
        Load only comments data
        
        Args:
            limit_per_event: Number of comments to fetch per entity
            events_only: If True, only fetch comments for events. If False, fetch for markets too.
        
        Returns:
            Dict with success status, count of comments loaded, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"💬 Loading COMMENTS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'comments': 0, 'reactions': 0, 'error': None}
        
        try:
            # Check if events exist
            event_count = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM events WHERE active = 1")
            
            if not event_count or event_count['count'] == 0:
                self.logger.warning("⚠️  No active events found. Please load events first.")
                result['error'] = "No events available"
                return result
            
            if events_only:
                comments_result = self.fetch_comments_for_all_events(limit_per_event)
            else:
                # Fetch for both events and markets
                events_result = self.fetch_comments_for_all_events(limit_per_event)
                markets_result = self.fetch_comments_for_all_markets(limit_per_event)
                comments_result = {
                    'comments_fetched': events_result['comments_fetched'] + markets_result.get('comments_fetched', 0),
                    'reactions_fetched': events_result['reactions_fetched'] + markets_result.get('reactions_fetched', 0)
                }
            
            result['comments'] = comments_result['comments_fetched']
            result['reactions'] = comments_result['reactions_fetched']
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Comments loaded: {result['comments']}")
            self.logger.info(f"✅ Reactions loaded: {result['reactions']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading comments: {e}")
            
        return result