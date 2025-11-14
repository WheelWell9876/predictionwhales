"""
Polymarket Data Fetcher Orchestrator - WHALE FOCUSED
Main entry point for fetching all Polymarket data
Enhanced with comprehensive whale transaction and trading data fetching
Fixed deletion methods with proper connection handling
"""

import time
import schedule
import threading
from datetime import datetime
from typing import Dict, Optional, List
from backend.database_manager import DatabaseManager
from backend.events_manager import EventsManager
from backend.markets_manager import MarketsManager
from backend.series_manager import SeriesManager
from backend.tags_manager import TagsManager
from backend.users_manager import UsersManager
from backend.transactions_manager import TransactionsManager
from backend.comments_manager import CommentsManager
from backend.config import Config
import logging
import gc  # For garbage collection

class PolymarketDataFetcher:
    """Main orchestrator for fetching Polymarket data with enhanced whale transaction support"""

    def __init__(self):
        self.logger = self._setup_logger()
        self.db_manager = DatabaseManager()
        self.events_manager = EventsManager()
        self.markets_manager = MarketsManager()
        self.series_manager = SeriesManager()
        self.tags_manager = TagsManager()
        self.users_manager = UsersManager()
        self.transactions_manager = TransactionsManager()
        self.comments_manager = CommentsManager()

        # WebSocket connection (for future implementation)
        self.ws_connection = None

    def _setup_logger(self):
        """Setup logger for the orchestrator"""
        logger = logging.getLogger('PolymarketDataFetcher')
        logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if not logger.handlers:
            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)

            # File handler
            fh = logging.FileHandler(Config.LOG_FILE)
            fh.setLevel(logging.DEBUG)

            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            fh.setFormatter(formatter)

            logger.addHandler(ch)
            logger.addHandler(fh)

        return logger

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
            self.comments_manager
        ]
        
        for manager in managers:
            try:
                if hasattr(manager, 'close_connection'):
                    manager.close_connection()
            except:
                pass
        
        # Force garbage collection to clean up any lingering connections
        gc.collect()
        
        # Small delay to ensure connections are closed
        time.sleep(0.5)

    def delete_transactions_only(self) -> Dict:
        """Delete only transactions and related trading data with proper connection handling"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TRANSACTIONS & TRADING Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # First, close all existing connections
            self._close_all_connections()
            
            # Create a fresh database connection for deletion
            import sqlite3
            conn = sqlite3.connect(
                self.db_manager.db_path,
                timeout=30.0,
                isolation_level='EXCLUSIVE'  # Get exclusive lock
            )
            
            try:
                cursor = conn.cursor()
                
                # Enable WAL mode for better handling
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                
                # Get counts before deletion
                tables_to_clear = [
                    'transactions',
                    'user_activity', 
                    'user_trades',
                    'user_positions_current',
                    'user_positions_closed',
                    'user_values'
                ]
                
                counts = {}
                for table in tables_to_clear:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all transaction-related data
                for table in tables_to_clear:
                    self.logger.info(f"  Deleting from {table}: {counts[table]} records")
                    cursor.execute(f"DELETE FROM {table}")
                
                # Commit the transaction
                conn.commit()
                
                # Calculate total deleted
                total_deleted = sum(counts.values())
                
                result['deleted'] = total_deleted
                result['success'] = True
                
                self.logger.info(f"\n✅ Deleted transaction and trading data:")
                for table, count in counts.items():
                    if count > 0:
                        self.logger.info(f"   {table}: {count:,}")
                self.logger.info(f"   Total deleted: {total_deleted:,}")
                
            finally:
                # Always close the connection
                conn.close()
                
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                result['error'] = "Database is locked. Please ensure no other processes are accessing the database."
                self.logger.error(f"❌ Database is locked. Try closing any other programs accessing the database.")
            else:
                result['error'] = str(e)
                self.logger.error(f"❌ Error deleting transactions: {e}")
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting transactions: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            self.transactions_manager = TransactionsManager()
            
        return result

    # ============== OTHER SELECTIVE DELETION METHODS (ALSO FIXED) ==============
    
    def delete_tags_only(self) -> Dict:
        """Delete only tags data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TAGS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Get current count
            before_count = self.db_manager.get_table_count('tags')
            
            # Delete from related tables first
            self.db_manager.delete_records('event_tags', commit=True)
            self.db_manager.delete_records('market_tags', commit=True)
            self.db_manager.delete_records('series_tags', commit=True)
            self.db_manager.delete_records('collection_tags', commit=True)
            self.db_manager.delete_records('tag_relationships', commit=True)
            
            # Delete tags
            deleted = self.db_manager.delete_records('tags', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} tags and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting tags: {e}")
            
        return result
    
    def delete_series_only(self) -> Dict:
        """Delete only series data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting SERIES Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Get current count
            before_count = self.db_manager.get_table_count('series')
            
            # Delete from related tables first
            self.db_manager.delete_records('series_events', commit=True)
            self.db_manager.delete_records('series_collections', commit=True)
            self.db_manager.delete_records('series_tags', commit=True)
            
            # Delete series and collections
            self.db_manager.delete_records('collections', commit=True)
            deleted = self.db_manager.delete_records('series', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} series and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting series: {e}")
            
        return result
    
    def delete_events_only(self, keep_active: bool = True) -> Dict:
        """Delete events data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"🗑️  Deleting {'CLOSED' if keep_active else 'ALL'} EVENTS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            if keep_active:
                # Delete only closed/inactive events
                deleted = self.db_manager.remove_closed_events()
                result['deleted'] = deleted
            else:
                # Get current count
                before_count = self.db_manager.get_table_count('events')
                
                # Delete all events and cascade
                self.db_manager.delete_records('event_tags', commit=True)
                self.db_manager.delete_records('series_events', commit=True)
                self.db_manager.delete_records('event_live_volume', commit=True)
                self.db_manager.delete_records('comments', commit=True)
                self.db_manager.delete_records('markets', commit=True)
                deleted = self.db_manager.delete_records('events', commit=True)
                result['deleted'] = before_count
            
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} events and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting events: {e}")
            
        return result
    
    def delete_markets_only(self) -> Dict:
        """Delete only markets data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting MARKETS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Get current count
            before_count = self.db_manager.get_table_count('markets')
            
            # Delete from related tables first
            self.db_manager.delete_records('market_tags', commit=True)
            self.db_manager.delete_records('market_open_interest', commit=True)
            self.db_manager.delete_records('market_holders', commit=True)
            
            # Delete markets
            deleted = self.db_manager.delete_records('markets', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} markets and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting markets: {e}")
            
        return result
    
    def delete_users_only(self) -> Dict:
        """Delete only users data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting USERS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Get current count
            before_count = self.db_manager.get_table_count('users')
            
            # Delete from related tables first
            self.db_manager.delete_records('user_positions_current', commit=True)
            self.db_manager.delete_records('user_positions_closed', commit=True)
            self.db_manager.delete_records('user_trades', commit=True)
            self.db_manager.delete_records('user_values', commit=True)
            self.db_manager.delete_records('user_activity', commit=True)
            self.db_manager.delete_records('market_holders', commit=True)
            
            # Delete users
            deleted = self.db_manager.delete_records('users', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} users and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting users: {e}")
            
        return result
    
    def delete_comments_only(self) -> Dict:
        """Delete only comments data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting COMMENTS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Get current count
            before_count = self.db_manager.get_table_count('comments')
            
            # Delete from related tables first
            self.db_manager.delete_records('comment_reactions', commit=True)
            
            # Delete comments
            deleted = self.db_manager.delete_records('comments', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} comments and reactions")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting comments: {e}")
            
        return result

    # ============== SELECTIVE LOADING METHODS (keeping these as-is) ==============
    
    def load_tags_only(self) -> Dict:
        """Load only tags data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️  Loading TAGS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            tags = self.tags_manager.fetch_all_tags()
            result['count'] = len(tags)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Tags loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading tags: {e}")
            
        return result
    
    def load_series_only(self) -> Dict:
        """Load only series data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📚 Loading SERIES Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            series = self.series_manager.fetch_all_series()
            result['count'] = len(series)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Series loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading series: {e}")
            
        return result
    
    def load_events_only(self, closed: bool = False) -> Dict:
        """Load only events data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"📅 Loading {'ALL' if closed else 'ACTIVE'} EVENTS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            events = self.events_manager.fetch_all_events(closed=closed)
            result['count'] = len(events)
            result['success'] = True
            
            # Clean up closed events if not fetching them
            if not closed:
                self.logger.info("🧹 Cleaning up closed events...")
                self.events_manager.remove_closed_events()
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Events loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading events: {e}")
            
        return result
    
    def load_markets_only(self, event_ids: List[str] = None) -> Dict:
        """Load only markets data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 Loading MARKETS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            if event_ids:
                # Load markets for specific events
                events = [{'id': eid} for eid in event_ids]
            else:
                # Load markets for all active events
                events = self.db_manager.fetch_all("""
                    SELECT id, slug FROM events 
                    WHERE active = 1
                """)
                
                if not events:
                    self.logger.warning("⚠️  No active events found. Loading events first...")
                    events_result = self.load_events_only(closed=False)
                    if events_result['success']:
                        events = self.db_manager.fetch_all("""
                            SELECT id, slug FROM events 
                            WHERE active = 1
                        """)
            
            markets = self.markets_manager.fetch_all_markets_from_events(events)
            result['count'] = len(markets)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Markets loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading markets: {e}")
            
        return result
    
    def load_users_only(self, whales_only: bool = True) -> Dict:
        """Load only users data (whales by default)"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"👥 Loading {'WHALE' if whales_only else 'ALL'} USERS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'whales_found': 0, 'enriched': 0, 'error': None}
        
        try:
            # Fetch top holders from all markets
            self.logger.info("🔍 Fetching top holders from all markets...")
            holders_result = self.users_manager.fetch_top_holders_for_all_markets()
            result['whales_found'] = holders_result['total_whales_found']
            
            # Enrich whale profiles
            if result['whales_found'] > 0:
                self.logger.info("📈 Enriching whale profiles...")
                enrich_result = self.users_manager.enrich_all_whale_users()
                result['enriched'] = enrich_result['total_whales_enriched']
            
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Whales found: {result['whales_found']}")
            self.logger.info(f"✅ Profiles enriched: {result['enriched']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading users: {e}")
            
        return result
    
    def load_transactions_only(self, comprehensive: bool = True) -> Dict:
        """
        Load transactions data with comprehensive whale data fetching
        Now includes: positions, activity, portfolio values, closed positions, and trades
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("💰 Loading TRANSACTIONS & WHALE DATA")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        
        if comprehensive:
            # Use the new comprehensive whale data fetching
            self.logger.info("Using comprehensive whale data fetching...")
            self.logger.info("This will fetch:")
            self.logger.info("  • Whale transactions")
            self.logger.info("  • Current positions")
            self.logger.info("  • User activity")
            self.logger.info("  • Portfolio values")
            self.logger.info("  • Closed positions (>$500)")
            self.logger.info("  • User trades")
            
            try:
                result = self.transactions_manager.fetch_comprehensive_whale_data()
                result['success'] = True
                
                elapsed_time = time.time() - start_time
                self.logger.info(f"⏱️  Time taken: {elapsed_time/60:.2f} minutes")
                
                return result
                
            except Exception as e:
                self.logger.error(f"❌ Error in comprehensive whale data fetch: {e}")
                return {'success': False, 'error': str(e)}
        
        else:
            # Legacy method - just fetch basic transactions
            self.logger.info("Using legacy transaction fetching...")
            result = {'success': False, 'transactions': 0, 'error': None}
            
            try:
                # Fetch recent whale transactions
                txns = self.transactions_manager.fetch_recent_whale_transactions()
                result['transactions'] = txns
                result['success'] = True
                
                elapsed_time = time.time() - start_time
                self.logger.info(f"✅ Transactions loaded: {result['transactions']}")
                self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
                
            except Exception as e:
                result['error'] = str(e)
                self.logger.error(f"❌ Error loading transactions: {e}")
            
            return result
    
    def load_comments_only(self, limit_per_event: int = 15) -> Dict:
        """Load only comments data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("💬 Loading COMMENTS Only")
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
            
            comments_result = self.comments_manager.fetch_comments_for_all_events(limit_per_event=limit_per_event)
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

    # Keep the rest of your methods (initial_data_load, daily_update, etc.) as they were...
    # I'm focusing just on the deletion fixes here

    def get_statistics(self) -> Dict:
        """Get database statistics"""
        return self.db_manager.get_statistics()

    def reset_database(self):
        """Reset the database"""
        # Close all connections first
        self._close_all_connections()
        
        # Now reset
        self.db_manager.reset_database()