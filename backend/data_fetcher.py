"""
Polymarket Data Fetcher Orchestrator - WHALE FOCUSED
Main entry point for fetching all Polymarket data
Initial load focused on high-value users (whales) and their complete profiles
"""

import time
import schedule
import threading
from datetime import datetime
from typing import Dict, Optional
from backend.database_manager import DatabaseManager
from backend.events_manager import EventsManager
from backend.markets_manager import MarketsManager
from backend.series_manager import SeriesManager
from backend.tags_manager import TagsManager
from backend.users_manager import UsersManager
from backend.transactions_manager import TransactionsManager
from backend.config import Config
import logging

class PolymarketDataFetcher:
    """Main orchestrator for fetching Polymarket data"""

    def __init__(self):
        self.logger = self._setup_logger()
        self.db_manager = DatabaseManager()
        self.events_manager = EventsManager()
        self.markets_manager = MarketsManager()
        self.series_manager = SeriesManager()
        self.tags_manager = TagsManager()
        self.users_manager = UsersManager()
        self.transactions_manager = TransactionsManager()

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
                self.logger.info("\n📌 Phase 1: Fetching Tags...")
                tags = self.tags_manager.fetch_all_tags()
                self.logger.info(f"✅ Tags fetched: {len(tags)}")

            # 2. Fetch all ACTIVE events only
            self.logger.info("\n📈 Phase 2: Fetching Active Events...")
            events = self.events_manager.fetch_all_events(closed=False)
            self.logger.info(f"✅ Active events fetched: {len(events)}")

            # 3. Fetch markets for all events
            self.logger.info("\n💹 Phase 3: Fetching Markets...")
            markets = self.markets_manager.fetch_all_markets_from_events(events)
            self.logger.info(f"✅ Markets fetched: {len(markets)}")

            # 4. Fetch series if enabled
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Phase 4: Fetching Series...")
                series = self.series_manager.fetch_all_series()
                self.logger.info(f"✅ Series fetched: {len(series)}")

            # ========== NEW WHALE-FOCUSED APPROACH ==========
            # 5. Fetch top 25 holders from ALL active markets
            self.logger.info("\n🐋 Phase 5: Fetching Top Holders from ALL Markets...")
            self.logger.info("   Criteria: $1000+ wallet OR $250+ position")
            
            result = self.users_manager.fetch_top_holders_for_all_markets()
            
            self.logger.info(f"✅ Markets processed: {result['total_markets_processed']}")
            self.logger.info(f"✅ Whale users found: {result['total_whales_found']}")

            # 6. Fetch complete profiles for ALL whales
            self.logger.info("\n🔍 Phase 6: Enriching Complete Whale Profiles...")
            self.logger.info("   For each whale:")
            self.logger.info("     - Trade history")
            self.logger.info("     - Activity history")
            self.logger.info("     - Wallet value")
            self.logger.info("     - Current positions")
            self.logger.info("     - Closed positions")
            self.logger.info("     - Comments and reactions")
            
            enrich_result = self.users_manager.enrich_all_whale_users()
            
            self.logger.info(f"✅ Enriched {enrich_result['total_whales_enriched']} whale profiles")
            self.logger.info(f"   Errors: {enrich_result['errors']}")

            # 7. Fetch whale transactions from top markets
            if Config.FETCH_TRANSACTIONS:
                self.logger.info("\n💰 Phase 7: Fetching Whale Transactions...")
                
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
                
                # Fetch global whale transactions
                self.logger.info("   Fetching global whale transactions...")
                whale_txs = self.transactions_manager.fetch_whale_transactions(
                    min_size=1000,  # $1000+ trades
                    limit=500
                )
                self.logger.info(f"✅ Whale transactions fetched: {len(whale_txs)}")

            elapsed_time = time.time() - start_time

            # Get final statistics
            stats = self.db_manager.get_statistics()

            # Print summary
            self.logger.info("\n" + "=" * 60)
            self.logger.info("🎉 INITIAL LOAD COMPLETE!")
            self.logger.info("=" * 60)
            self.logger.info(f"⏱️  Time taken: {elapsed_time/60:.2f} minutes")
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
                self.logger.info("\n📌 Updating Tags...")
                results['tags'] = self.tags_manager.daily_scan()

            # 2. Update events
            self.logger.info("\n📈 Updating Events...")
            results['events'] = self.events_manager.daily_scan()

            # 3. Update markets
            self.logger.info("\n💹 Updating Markets...")
            # Get recent events
            recent_events = self.db_manager.fetch_all("""
                SELECT id, slug FROM events 
                WHERE active = 1 
                ORDER BY updated_at DESC 
                LIMIT 100
            """)
            results['markets'] = self.markets_manager.fetch_all_markets_from_events(recent_events)

            # 4. Update series
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Updating Series...")
                results['series'] = self.series_manager.daily_scan()

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

            # 6. Update whale profiles
            self.logger.info("\n📊 Updating Whale Profiles...")
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

        self.logger.info("✅ Scheduler started")

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