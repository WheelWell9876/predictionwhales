"""
Polymarket Data Fetcher Orchestrator - WHALE FOCUSED
Main entry point for fetching all Polymarket data
Initial load focused on high-value users (whales) and their complete profiles
Enhanced with selective loading and deletion capabilities
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

class PolymarketDataFetcher:
    """Main orchestrator for fetching Polymarket data with selective operations"""

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

    # ============== SELECTIVE LOADING METHODS ==============
    
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
    
    def load_transactions_only(self, limit_markets: int = 20) -> Dict:
        """Load only transactions data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("💰 Loading TRANSACTIONS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'markets_processed': 0, 'transactions': 0, 'error': None}
        
        try:
            # Get top markets by volume
            top_markets = self.db_manager.fetch_all(f"""
                SELECT id, condition_id, question, volume
                FROM markets
                WHERE active = 1 AND condition_id IS NOT NULL
                ORDER BY volume DESC
                LIMIT {limit_markets}
            """)
            
            if not top_markets:
                self.logger.warning("⚠️  No active markets found. Please load markets first.")
                result['error'] = "No markets available"
                return result
            
            for market in top_markets:
                self.logger.info(f"   Processing: {market['question'][:50]}...")
                txns = self.transactions_manager.fetch_market_transactions(
                    market_id=market['id'],
                    condition_id=market['condition_id'],
                    limit=100
                )
                result['transactions'] += len(txns) if txns else 0
                result['markets_processed'] += 1
                time.sleep(Config.RATE_LIMIT_DELAY)
            
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Markets processed: {result['markets_processed']}")
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

    # ============== SELECTIVE DELETION METHODS ==============
    
    def delete_tags_only(self) -> Dict:
        """Delete only tags data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TAGS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
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
    
    def delete_transactions_only(self) -> Dict:
        """Delete only transactions data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TRANSACTIONS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Get current count
            before_count = self.db_manager.get_table_count('transactions')
            
            # Delete transactions
            deleted = self.db_manager.delete_records('transactions', commit=True)
            result['deleted'] = before_count
            result['success'] = True
            
            self.logger.info(f"✅ Deleted {result['deleted']} transactions")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting transactions: {e}")
            
        return result
    
    def delete_comments_only(self) -> Dict:
        """Delete only comments data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting COMMENTS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
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

    # ============== ORIGINAL METHODS (PRESERVED) ==============
    
    def initial_data_load(self, reset_database: bool = False):
        """
        Perform initial data load from Polymarket APIs
        WHALE-FOCUSED: Gets top holders from all markets and their complete profiles
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting WHALE-FOCUSED Polymarket data load...")
        self.logger.info(f"Time: {datetime.now()}")
        self.logger.info("=" * 60)

        # Reset database if requested
        if reset_database:
            self.logger.warning("Resetting database...")
            self.db_manager.reset_database()

        # Backup database before major operation
        backup_path = self.db_manager.backup_database()
        self.logger.info(f"Database backed up to: {backup_path}")

        start_time = time.time()

        try:
            # 1. Fetch all tags first (they're used by other entities)
            if Config.FETCH_TAGS:
                self.logger.info("\n🏷️ Phase 1: Fetching Tags...")
                tags = self.tags_manager.fetch_all_tags()
                self.logger.info(f"Tags fetched: {len(tags)}")
                
                # Clean up closed events after tags
                self.logger.info("   Cleaning up closed events...")
                self.events_manager.remove_closed_events()

            # 2. Fetch all ACTIVE events only
            self.logger.info("📅 Phase 2: Fetching Active Events...")
            events = self.events_manager.fetch_all_events(closed=False)
            self.logger.info(f"🔥 Active events fetched: {len(events)}")
            
            # Clean up closed events after events fetch
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 3. Fetch markets for all events
            self.logger.info("\n📊 Phase 3: Fetching Markets...")
            markets = self.markets_manager.fetch_all_markets_from_events(events)
            self.logger.info(f"📈 Markets fetched: {len(markets)}")
            
            # Clean up closed events after markets fetch
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 3.5. Clean up closed/inactive events and their data
            self.logger.info("\n🧹 Phase 3.5: Cleaning Up Closed Events...")
            removed_count = self.db_manager.remove_closed_events()
            self.logger.info(f"🗑️ Removed {removed_count} closed events and associated data")
            
            # Clean up closed events after cleanup phase
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 4. Fetch series if enabled
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Phase 4: Fetching Series...")
                series = self.series_manager.fetch_all_series()
                self.logger.info(f"📖 Series fetched: {len(series)}")
                
                # Clean up closed events after series fetch
                self.logger.info("   Cleaning up closed events...")
                self.events_manager.remove_closed_events()

            # ========== NEW WHALE-FOCUSED APPROACH ==========
            # 5. Fetch top 25 holders from ALL active markets
            self.logger.info("\n🐋 Phase 5: Fetching Top Holders from ALL Markets...")
            self.logger.info("   Criteria: $1000+ wallet OR $250+ position")
            
            result = self.users_manager.fetch_top_holders_for_all_markets()
            
            self.logger.info(f"📊 Markets processed: {result['total_markets_processed']}")
            self.logger.info(f"🐋 Whale users found: {result['total_whales_found']}")
            
            # Clean up closed events after holders fetch
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 6. Fetch complete profiles for ALL whales
            self.logger.info("\n💎 Phase 6: Enriching Complete Whale Profiles...")
            self.logger.info("   For each whale:")
            self.logger.info("     - Trade history")
            self.logger.info("     - Activity history")
            self.logger.info("     - Wallet value")
            self.logger.info("     - Current positions")
            self.logger.info("     - Closed positions")
            
            enrich_result = self.users_manager.enrich_all_whale_users()
            
            self.logger.info(f"✅ Enriched {enrich_result['total_whales_enriched']} whale profiles")
            self.logger.info(f"   Errors: {enrich_result['errors']}")
            
            # Clean up closed events after whale enrichment
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 7. Fetch comments for events (top 15 per event)
            self.logger.info("\n💬 Phase 7: Fetching Comments for Events...")
            comments_result = self.comments_manager.fetch_comments_for_all_events(limit_per_event=15)
            self.logger.info(f"💬 Comments fetched: {comments_result['comments_fetched']}")
            self.logger.info(f"   Reactions fetched: {comments_result['reactions_fetched']}")
            
            # Clean up closed events after comments fetch
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 8. Fetch whale transactions from top markets
            if Config.FETCH_TRANSACTIONS:
                self.logger.info("\n💰 Phase 8: Fetching Whale Transactions...")
                
                # Get top 20 markets by volume
                top_markets = self.db_manager.fetch_all("""
                    SELECT id, condition_id, question, volume
                    FROM markets
                    WHERE active = 1 AND condition_id IS NOT NULL
                    ORDER BY volume DESC
                    LIMIT 20
                """)
                
                for market in top_markets:
                    self.logger.info(f"   Fetching transactions for: {market['question'][:50]}...")
                    self.transactions_manager.fetch_market_transactions(
                        market_id=market['id'],
                        condition_id=market['condition_id'],
                        limit=100
                    )
                    time.sleep(Config.RATE_LIMIT_DELAY)

            # Final cleanup
            self.logger.info("\n🧹 Final Cleanup: Removing any remaining closed events...")
            final_removed = self.db_manager.remove_closed_events()
            if final_removed > 0:
                self.logger.info(f"🗑️ Final cleanup removed {final_removed} closed events")

            elapsed_time = time.time() - start_time

            # Get final statistics
            stats = self.db_manager.get_statistics()

            # Print summary
            self.logger.info("\n" + "=" * 60)
            self.logger.info("INITIAL LOAD COMPLETE!")
            self.logger.info("=" * 60)
            self.logger.info(f"⏱️ Time taken: {elapsed_time/60:.2f} minutes")
            self.logger.info(f"\n📊 Final Statistics:")
            for table, count in stats.items():
                self.logger.info(f"   {table:<30} {count:>10,} records")
            
            # Whale-specific stats
            whale_count = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
            self.logger.info(f"\n🐋 Whale Users: {whale_count['count']:,}")
            
            avg_whale_value = self.db_manager.fetch_one("""
                SELECT AVG(total_value) as avg_value 
                FROM users 
                WHERE is_whale = 1 AND total_value > 0
            """)
            if avg_whale_value and avg_whale_value['avg_value']:
                self.logger.info(f"💰 Average Whale Wallet: ${avg_whale_value['avg_value']:,.2f}")

            self.logger.info("=" * 60)

            return True

        except Exception as e:
            self.logger.error(f"Error during initial data load: {e}")
            self.logger.exception("Full traceback:")
            return False

    def daily_update(self) -> Dict:
        """
        Perform daily update scan
        Updates existing data and fetches new markets/whales
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting daily update scan...")
        self.logger.info("=" * 60)

        results = {
            'events': 0,
            'markets': 0,
            'tags': 0,
            'series': 0,
            'new_whales': 0,
            'whale_updates': 0
        }

        try:
            # 1. Update tags
            if Config.FETCH_TAGS:
                self.logger.info("\n🏷️ Updating Tags...")
                results['tags'] = self.tags_manager.daily_scan()
                
                # Clean up closed events after tags update
                self.logger.info("   Cleaning up closed events...")
                self.events_manager.remove_closed_events()

            # 2. Update events
            self.logger.info("\n📅 Updating Events...")
            results['events'] = self.events_manager.daily_scan()
            
            # Clean up closed events after events update
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 3. Update markets
            self.logger.info("\n📊 Updating Markets...")
            # Get recent events
            recent_events = self.db_manager.fetch_all("""
                SELECT id, slug FROM events 
                WHERE active = 1 
                ORDER BY updated_at DESC 
                LIMIT 100
            """)
            results['markets'] = self.markets_manager.fetch_all_markets_from_events(recent_events)
            
            # Clean up closed events after markets update
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 4. Update series
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Updating Series...")
                results['series'] = self.series_manager.daily_scan()
                
                # Clean up closed events after series update
                self.logger.info("   Cleaning up closed events...")
                self.events_manager.remove_closed_events()

            # 5. Check for new whales in updated markets
            self.logger.info("\n🐋 Checking for New Whales...")
            before_count = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
            
            # Scan top 50 markets for new whales
            top_markets = self.db_manager.fetch_all("""
                SELECT id FROM markets 
                WHERE active = 1 
                ORDER BY updated_at DESC 
                LIMIT 50
            """)
            
            # This will find and add new whales
            whale_result = self.users_manager.fetch_top_holders_for_all_markets()
            
            after_count = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
            results['new_whales'] = after_count['count'] - before_count['count']
            
            if results['new_whales'] > 0:
                self.logger.info(f"✅ Found {results['new_whales']} new whales")
            
            # Clean up closed events after whale check
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            # 6. Update whale profiles
            self.logger.info("\n💎 Updating Whale Profiles...")
            # Update profiles for whales that haven't been updated recently
            stale_whales = self.db_manager.fetch_all("""
                SELECT proxy_wallet FROM users 
                WHERE is_whale = 1 
                AND (last_updated IS NULL OR last_updated < datetime('now', '-1 day'))
                LIMIT 50
            """)
            
            if stale_whales:
                wallet_list = [w['proxy_wallet'] for w in stale_whales]
                update_result = self.users_manager.batch_enrich_whales(wallet_list)
                results['whale_updates'] = update_result['total_enriched']
            else:
                results['whale_updates'] = 0
            
            # Clean up closed events after whale updates
            self.logger.info("   Cleaning up closed events...")
            self.events_manager.remove_closed_events()

            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ Daily update complete!")
            self.logger.info(f"   Events: {results['events']}")
            self.logger.info(f"   Markets: {results['markets']}")
            self.logger.info(f"   New Whales: {results['new_whales']}")
            self.logger.info(f"   Whale Updates: {results['whale_updates']}")
            self.logger.info("=" * 60)

            return results

        except Exception as e:
            self.logger.error(f"Error during daily update: {e}")
            return results

    def start_scheduler(self):
        """
        Start the scheduler for daily updates
        """
        if not Config.ENABLE_DAILY_SCAN:
            self.logger.info("Daily scanning is disabled in config")
            return

        self.logger.info(f"Scheduling daily scans at {Config.DAILY_SCAN_TIME}")

        # Schedule daily update
        schedule.every().day.at(Config.DAILY_SCAN_TIME).do(self.daily_update)

        # Run scheduler in background
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)

        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()

        self.logger.info("⏰ Scheduler started")

    def get_statistics(self) -> Dict:
        """Get database statistics"""
        return self.db_manager.get_statistics()

    def reset_database(self):
        """Reset the database"""
        self.db_manager.reset_database()


def main():
    """Main function"""
    fetcher = PolymarketDataFetcher()
    fetcher.initial_data_load()


if __name__ == "__main__":
    main()