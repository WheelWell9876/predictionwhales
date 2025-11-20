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
        # For now, we'll create stub managers to avoid import errors
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
        """Verify that critical tables like event_tags exist"""
        self.logger.info("Verifying critical tables...")
        
        critical_tables = ['events', 'event_tags', 'markets', 'market_tags', 
                          'series', 'series_tags', 'tags']
        
        missing_tables = []
        for table in critical_tables:
            if not self.db_manager.table_exists(table):
                missing_tables.append(table)
                self.logger.error(f"❌ Critical table missing: {table}")
            else:
                self.logger.debug(f"✅ Table verified: {table}")
        
        if missing_tables:
            self.logger.error(f"Missing critical tables: {', '.join(missing_tables)}")
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
                self.logger.info("✅ All critical tables created successfully!")
        else:
            self.logger.info("✅ All critical tables verified!")

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

    # ============== STUB OPERATIONS FOR NOW ==============
    # These would normally delegate to the respective managers
    
    def load_events_only(self, closed: bool = False) -> Dict:
        """Load only events data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📥 Loading EVENTS")
        self.logger.info("=" * 60)
        
        # Verify event_tags table exists before attempting to load
        if not self.db_manager.table_exists('event_tags'):
            self.logger.error("❌ event_tags table does not exist!")
            return {'success': False, 'error': 'event_tags table missing'}
        
        # For now, return a stub response since we don't have the actual EventsManager
        # In the real implementation, this would call self.events_manager.load_events_only(closed)
        self.logger.info("Note: This is a stub implementation. Actual EventsManager needs to be imported.")
        
        # Try to import the actual EventsManager
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
    
    # ============== DAILY SCAN OPERATION ==============
    
    def run_daily_scan(self) -> Dict:
        """Run complete daily scan of all data sections"""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("🔄 RUNNING DAILY SCAN")
        self.logger.info("=" * 80)
        self.logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = {}
        start_time = time.time()
        
        # Verify all critical tables first
        self._verify_critical_tables()
        
        sections = [
            ('events', lambda: self.load_events_only(closed=False)),
            ('markets', lambda: self.load_markets_only()),
            ('series', lambda: self.load_series_only()),
            ('tags', lambda: self.load_tags_only()),
            ('users', lambda: self.load_users_only(whales_only=True)),
            ('comments', lambda: self.load_comments_only(limit_per_event=15)),
            ('positions', lambda: self.load_positions_only(whale_users_only=True)),
            ('transactions', lambda: self.load_transactions_only(comprehensive=True))
        ]
        
        for section_name, loader_func in sections:
            if getattr(Config, f'FETCH_{section_name.upper()}', True):
                self.logger.info(f"\n📌 Loading {section_name}...")
                section_start = time.time()
                
                try:
                    result = loader_func()
                    results[section_name] = result
                    
                    elapsed = time.time() - section_start
                    if result.get('success', False):
                        self.logger.info(f"✅ {section_name} loaded in {elapsed:.2f}s")
                    else:
                        self.logger.warning(f"⚠️ {section_name} failed: {result.get('error')}")
                except Exception as e:
                    self.logger.error(f"❌ Error loading {section_name}: {e}")
                    results[section_name] = {'success': False, 'error': str(e)}
            else:
                self.logger.info(f"⏩ Skipping {section_name} (disabled in config)")
        
        total_elapsed = time.time() - start_time
        
        # Summary
        successful = sum(1 for r in results.values() if r.get('success', False))
        failed = len(results) - successful
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info("📊 DAILY SCAN COMPLETE")
        self.logger.info("=" * 80)
        self.logger.info(f"✅ Successful: {successful}/{len(results)}")
        if failed > 0:
            self.logger.info(f"❌ Failed: {failed}")
        self.logger.info(f"⏱️ Total time: {total_elapsed:.2f} seconds")
        
        # Run database optimization if everything succeeded
        if failed == 0:
            try:
                self.logger.info("\n🔧 Running database optimization...")
                self.db_manager.analyze()
                self.logger.info("✅ Database optimized")
            except Exception as e:
                self.logger.error(f"❌ Database optimization failed: {e}")
        
        return {
            'success': failed == 0,
            'results': results,
            'elapsed': total_elapsed,
            'successful': successful,
            'failed': failed
        }