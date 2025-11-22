#!/usr/bin/env python3
"""
Polymarket Data Fetcher - Fixed Version
Main entry point for fetching all Polymarket data
WITH PROPER DATABASE INITIALIZATION FOR EVENT_TAGS TABLE
"""

import time
import logging
import gc
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.config import Config
from backend.database.database_manager import DatabaseManager

class PolymarketDataFetcher:
    """Main orchestrator for fetching Polymarket data"""

    def __init__(self):
        self.logger = self._setup_logger()
        
        # CRITICAL: Initialize database manager first to ensure tables are created
        self.logger.info("Initializing PolymarketDataFetcher...")
        self.db_manager = DatabaseManager()
        
        # Verify event_tags table exists specifically
        self._verify_critical_tables()
        
        # Initialize managers (these would be imported from their respective modules)
        self.events_manager = None
        self.markets_manager = None
        self.series_manager = None
        self.tags_manager = None
        self.users_manager = None
        self.transactions_manager = None
        self.comments_manager = None
        self.positions_manager = None
        
        self.logger.info("✅ PolymarketDataFetcher initialized successfully")

    def _setup_logger(self):
        """Setup logger for the orchestrator"""
        logger = logging.getLogger('PolymarketDataFetcher')
        logger.setLevel(logging.DEBUG)  # Set to DEBUG for better debugging

        # Avoid duplicate handlers
        if not logger.handlers:
            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)

            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            logger.addHandler(ch)

        return logger

    def _verify_critical_tables(self):
        """Verify that ALL tables in the schema exist"""
        self.logger.info("Verifying all database tables...")

        # Get all expected tables from the schema
        all_tables = [
            # Core tables
            'events', 'markets', 'image_optimized', 'categories', 'collections',
            'series', 'tags', 'event_creators', 'chats', 'templates', 'users', 'comments',
            # Tracking tables
            'event_live_volume', 'market_open_interest', 'market_holders',
            # Relationship tables
            'event_tags', 'market_tags', 'market_categories', 'event_series',
            'event_collections', 'event_categories', 'event_event_creators',
            'event_chats', 'event_templates', 'series_tags', 'series_categories',
            'series_collections', 'series_chats', 'series_events', 'tag_relationships',
            'comment_reactions',
            # User tables
            'user_activity', 'user_trades', 'user_positions_current',
            'user_positions_closed', 'user_values', 'transactions'
        ]

        missing_tables = []
        verified_count = 0

        for table in all_tables:
            if not self.db_manager.table_exists(table):
                missing_tables.append(table)
                self.logger.error(f"❌ Table missing: {table}")
            else:
                self.logger.debug(f"✅ Table verified: {table}")
                verified_count += 1

        if missing_tables:
            self.logger.error(f"Missing tables: {', '.join(missing_tables)}")
            self.logger.info("Attempting to reinitialize database schema...")
            self.db_manager.initialize_schema()

            # Verify again
            still_missing = []
            for table in missing_tables:
                if not self.db_manager.table_exists(table):
                    still_missing.append(table)

            if still_missing:
                raise RuntimeError(f"Failed to create tables: {', '.join(still_missing)}")
            else:
                self.logger.info(f"✅ All {len(all_tables)} tables created successfully!")
        else:
            self.logger.info(f"✅ All {verified_count} tables verified!")

    def _close_all_connections(self):
        """Close all database connections from all managers"""
        self.logger.info("Closing all database connections...")
        
        # Close connections from all managers
        managers = [
            self.db_manager,
            self.events_manager,
            self.markets_manager,
            self.series_manager,
            self.tags_manager,
            self.users_manager,
            self.transactions_manager,
            self.comments_manager,
            self.positions_manager
        ]
        
        for manager in managers:
            if manager is not None:
                try:
                    if hasattr(manager, 'close_connection'):
                        manager.close_connection()
                    # For EventsManager, use its special close method
                    if hasattr(manager, '_close_all_connections'):
                        manager._close_all_connections()
                except:
                    pass
        
        # Force garbage collection to clean up any lingering connections
        gc.collect()
        
        # Small delay to ensure connections are closed
        time.sleep(0.5)

    # ============== LOAD OPERATIONS ==============
    
    def load_events_only(self, closed: bool = False) -> Dict:
        """Load only events data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📥 Loading EVENTS")
        self.logger.info("=" * 60)
        
        # Verify event_tags table exists before attempting to load
        if not self.db_manager.table_exists('event_tags'):
            self.logger.error("❌ event_tags table does not exist!")
            return {'success': False, 'error': 'event_tags table missing'}
        
        try:
            from backend.events_manager import EventsManager
            self.events_manager = EventsManager()
            return self.events_manager.load_events_only(closed)
        except ImportError as e:
            self.logger.warning(f"Could not import EventsManager: {e}")
            return {
                'success': False, 
                'error': 'EventsManager not available',
                'message': 'Database tables are ready, but EventsManager module not found'
            }
    
    def load_markets_only(self, event_ids: List[str] = None) -> Dict:
        """Load only markets data"""
        try:
            from backend.markets_manager import MarketsManager
            self.markets_manager = MarketsManager()
            return self.markets_manager.load_markets_only(event_ids)
        except ImportError:
            return {'success': False, 'error': 'MarketsManager not available'}
    
    def load_series_only(self) -> Dict:
        """Load only series data"""
        try:
            from backend.series_manager import SeriesManager
            self.series_manager = SeriesManager()
            return self.series_manager.load_series_only()
        except ImportError:
            return {'success': False, 'error': 'SeriesManager not available'}
    
    def load_tags_only(self) -> Dict:
        """Load only tags data"""
        try:
            from backend.tags_manager import TagsManager
            self.tags_manager = TagsManager()
            return self.tags_manager.load_tags_only()
        except ImportError:
            return {'success': False, 'error': 'TagsManager not available'}
    
    def load_tag_relationships_only(self) -> Dict:
        """Load tag relationships from events"""
        try:
            from backend.tags_manager import TagsManager
            self.tags_manager = TagsManager()
            return self.tags_manager.load_tag_relationships_only()
        except ImportError:
            return {'success': False, 'error': 'TagsManager not available'}

    def load_tags_with_relationships(self) -> Dict:
        """Load tags and then load tag relationships - combined workflow"""
        try:
            from backend.tags_manager import TagsManager
            self.tags_manager = TagsManager()
            return self.tags_manager.load_tags_with_relationships()
        except ImportError:
            return {'success': False, 'error': 'TagsManager not available'}

    def load_users_only(self, whales_only: bool = True) -> Dict:
        """Load only users data"""
        try:
            from backend.users_manager import UsersManager
            self.users_manager = UsersManager()
            return self.users_manager.load_users_only(whales_only)
        except ImportError:
            return {'success': False, 'error': 'UsersManager not available'}
    
    def load_comments_only(self, limit_per_event: int = 15) -> Dict:
        """Load only comments data"""
        try:
            from backend.comments_manager import CommentsManager
            self.comments_manager = CommentsManager()
            return self.comments_manager.load_comments_only(limit_per_event)
        except ImportError:
            return {'success': False, 'error': 'CommentsManager not available'}
    
    def load_positions_only(self, whale_users_only: bool = True) -> Dict:
        """Load only positions data"""
        try:
            from backend.positions_manager import PositionsManager
            self.positions_manager = PositionsManager()
            return self.positions_manager.load_positions_only(whale_users_only)
        except ImportError:
            return {'success': False, 'error': 'PositionsManager not available'}
    
    def load_transactions_only(self, comprehensive: bool = True) -> Dict:
        """Load only transactions data"""
        try:
            from backend.transactions_manager import TransactionsManager
            self.transactions_manager = TransactionsManager()
            return self.transactions_manager.load_transactions_only(comprehensive)
        except ImportError:
            return {'success': False, 'error': 'TransactionsManager not available'}
    
    # ============== DELETE OPERATIONS ==============
    
    def delete_events_only(self, keep_active: bool = True) -> Dict:
        """Delete events data"""
        try:
            from backend.events_manager import EventsManager
            self.events_manager = EventsManager()
            return self.events_manager.delete_events_only(keep_active)
        except ImportError:
            return {'success': False, 'error': 'EventsManager not available'}
    
    def delete_markets_only(self) -> Dict:
        """Delete markets data"""
        try:
            from backend.markets_manager import MarketsManager
            self.markets_manager = MarketsManager()
            return self.markets_manager.delete_markets_only()
        except ImportError:
            return {'success': False, 'error': 'MarketsManager not available'}
    
    def delete_series_only(self) -> Dict:
        """Delete series data"""
        try:
            from backend.series_manager import SeriesManager
            self.series_manager = SeriesManager()
            return self.series_manager.delete_series_only()
        except ImportError:
            return {'success': False, 'error': 'SeriesManager not available'}
    
    def delete_tags_only(self) -> Dict:
        """Delete tags data"""
        try:
            from backend.tags_manager import TagsManager
            self.tags_manager = TagsManager()
            return self.tags_manager.delete_tags_only()
        except ImportError:
            return {'success': False, 'error': 'TagsManager not available'}
    
    def delete_tag_relationships_only(self) -> Dict:
        """Delete tag relationships data"""
        try:
            from backend.tags_manager import TagsManager
            self.tags_manager = TagsManager()
            return self.tags_manager.delete_tag_relationships_only()
        except ImportError:
            return {'success': False, 'error': 'TagsManager not available'}
    
    def delete_users_only(self) -> Dict:
        """Delete users data"""
        try:
            from backend.users_manager import UsersManager
            self.users_manager = UsersManager()
            return self.users_manager.delete_users_only()
        except ImportError:
            return {'success': False, 'error': 'UsersManager not available'}
    
    def delete_comments_only(self) -> Dict:
        """Delete comments data"""
        try:
            from backend.comments_manager import CommentsManager
            self.comments_manager = CommentsManager()
            return self.comments_manager.delete_comments_only()
        except ImportError:
            return {'success': False, 'error': 'CommentsManager not available'}
    
    def delete_positions_only(self) -> Dict:
        """Delete positions data"""
        try:
            from backend.positions_manager import PositionsManager
            self.positions_manager = PositionsManager()
            return self.positions_manager.delete_positions_only()
        except ImportError:
            return {'success': False, 'error': 'PositionsManager not available'}
    
    def delete_transactions_only(self) -> Dict:
        """Delete transactions data"""
        try:
            from backend.transactions_manager import TransactionsManager
            self.transactions_manager = TransactionsManager()
            return self.transactions_manager.delete_transactions_only()
        except ImportError:
            return {'success': False, 'error': 'TransactionsManager not available'}
    
    # ============== COMPREHENSIVE OPERATIONS ==============
    
    def load_all_data(self) -> Dict:
        """Load all data in the correct order"""
        results = {}

        # Load in dependency order:
        # 1. events first (base data)
        # 2. series second (needed for event-series relationships)
        # 3. tags third (needed for tag relationships)
        # 4. markets fourth (references events and creates event-series/event-tags relationships)
        steps = [
            ('events', lambda: self.load_events_only()),
            ('series', lambda: self.load_series_only()),
            ('tags', lambda: self.load_tags_only()),
            ('markets', lambda: self.load_markets_only()),
            ('tag_relationships', lambda: self.load_tag_relationships_only()),
            ('users', lambda: self.load_users_only()),
            ('comments', lambda: self.load_comments_only()),
            ('positions', lambda: self.load_positions_only()),
            ('transactions', lambda: self.load_transactions_only())
        ]

        for step_name, step_func in steps:
            self.logger.info(f"Loading {step_name}...")
            results[step_name] = step_func()
            if not results[step_name].get('success', False):
                self.logger.error(f"Failed to load {step_name}")

        return results

    def load_core_data(self) -> Dict:
        """Load core data (events, series, tags, markets) in the correct order"""
        results = {}

        # Load in dependency order for core data
        steps = [
            ('events', lambda: self.load_events_only()),
            ('series', lambda: self.load_series_only()),
            ('tags', lambda: self.load_tags_only()),
            ('markets', lambda: self.load_markets_only()),
        ]

        for step_name, step_func in steps:
            self.logger.info(f"Loading {step_name}...")
            results[step_name] = step_func()
            if not results[step_name].get('success', False):
                self.logger.error(f"Failed to load {step_name}")

        return results
    
    def get_statistics(self) -> Dict:
        """Get overall database statistics"""
        stats = {}
        
        # Get table counts
        tables = ['events', 'markets', 'series', 'tags', 'tag_relationships',
                 'users', 'comments', 'positions', 'transactions']
        
        for table in tables:
            try:
                count = self.db_manager.get_table_count(table)
                stats[table] = count
            except:
                stats[table] = 0
        
        return stats
    
    def cleanup(self):
        """Clean up resources and close connections"""
        self._close_all_connections()
        self.logger.info("Cleanup complete")


# Example usage
if __name__ == "__main__":
    fetcher = PolymarketDataFetcher()
    
    # Example: Load events
    result = fetcher.load_events_only()
    print(f"Events loaded: {result}")
    
    # Example: Get statistics
    stats = fetcher.get_statistics()
    print(f"Database statistics: {stats}")
    
    # Cleanup
    fetcher.cleanup()