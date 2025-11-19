"""
Tags Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing tag data with concurrent requests
"""

import requests
import json
import time
import sqlite3
import gc
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.fetch.entity.batch.batch_tags import BatchTagsManager
from backend.fetch.entity.id.id_tags import IdTagsManager
from backend.database.entity.store_tags import StoreTagsManager

class TagsManager:
    """Manager for tag-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchTagsManager()
        self.id_manager = IdTagsManager()
        self.store_manager = StoreTagsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()

    def fetch_all_tags(self, limit: int = 1000) -> List[Dict]:
        """
        Fetch all tags from the API
        
        Args:
            limit: Number of tags to fetch
        """
        return self.batch_manager.fetch_all_tags(limit)

    def fetch_tag_by_id(self, tag_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific tag
        """
        return self.id_manager.fetch_tag_by_id(tag_id)

    def fetch_tag_by_id_parallel(self, tag_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific tag with parallel sub-requests
        """
        return self.id_manager.fetch_tag_by_id_parallel(tag_id)

    def fetch_tag_relationships(self, tag_id: str) -> List[Dict]:
        """
        Fetch relationships for a specific tag
        """
        return self.batch_manager.fetch_tag_relationships(tag_id)

    def fetch_event_tags(self, event_id: str) -> List[Dict]:
        """
        Fetch tags for a specific event
        """
        return self.batch_manager.fetch_event_tags(event_id)

    def fetch_market_tags(self, market_id: str) -> List[Dict]:
        """
        Fetch tags for a specific market
        """
        return self.batch_manager.fetch_market_tags(market_id)

    def process_all_tags_detailed(self, use_parallel: bool = True, num_threads: int = 20):
        """
        Process all tags to fetch detailed information with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests
            num_threads: Number of concurrent threads (default: 20)
        """
        # Get all tag IDs from database
        tags = self.db_manager.fetch_all("SELECT id FROM tags ORDER BY id")
        
        self.logger.info(f"Processing {len(tags)} tags for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        relationships_count = 0
        lock = Lock()
        
        # Choose which fetch method to use
        fetch_method = self.fetch_tag_by_id_parallel if use_parallel else self.fetch_tag_by_id
        
        def process_tag(tag):
            nonlocal processed, errors, relationships_count
            try:
                result = fetch_method(tag['id'])
                
                # Fetch relationships
                relationships = self.fetch_tag_relationships(tag['id'])
                
                with lock:
                    processed += 1
                    relationships_count += len(relationships)
                    if processed % 50 == 0:
                        self.logger.info(f"Processed {processed}/{len(tags)} tags")
            except Exception as e:
                with lock:
                    errors += 1
                self.logger.error(f"Error processing tag {tag['id']}: {e}")
        
        # Process tags concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_tag, tags)
        
        self.logger.info(f"✅ Tag processing complete. Processed: {processed}, Errors: {errors}")
        self.logger.info(f"Total relationships found: {relationships_count}")

    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for tag updates
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        if not self.config.FETCH_TAGS:
            self.logger.info("Tag fetching disabled")
            return 0
        
        self.logger.info("Starting daily tag scan...")
        
        # Fetch all tags
        all_tags = self.fetch_all_tags()
        
        # Process detailed information and relationships
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
        stats['total_tags'] = self.db_manager.get_table_count('tags')
        
        # Tags with relationships
        result = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count
            FROM tag_relationships
        """)
        stats['tags_with_relationships'] = result['count'] if result else 0
        
        # Total relationships
        stats['total_relationships'] = self.db_manager.get_table_count('tag_relationships')
        
        return stats

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all tag manager database connections...")
        
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

    def delete_tags_only(self) -> Dict:
        """
        Delete tags data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TAGS Data")
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
                cursor.execute("SELECT COUNT(*) FROM tags")
                before_count = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all related data
                tables_to_clear = [
                    'event_tags',
                    'market_tags',
                    'series_tags',
                    'collection_tags',
                    'tag_relationships',
                    'tags'
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
            self.logger.info(f"✅ Deleted {result['deleted']} tags and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting tags: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_tags_only(self) -> Dict:
        """
        Load only tags data
        
        Returns:
            Dict with success status, count of tags loaded, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️  Loading TAGS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            tags = self.fetch_all_tags()
            result['count'] = len(tags)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Tags loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading tags: {e}")
            
        return result