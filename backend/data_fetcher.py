"""
Polymarket Data Fetcher Orchestrator
Main entry point for fetching all Polymarket data
Coordinates all managers and handles scheduling
"""

import time
import schedule
import threading
from datetime import datetime
from typing import Dict, Optional
from .database_manager import DatabaseManager
from .events_manager import EventsManager
from .markets_manager import MarketsManager
from .series_manager import SeriesManager
from .tags_manager import TagsManager
from .config import Config
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
        This should be run once when setting up the system
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting initial Polymarket data load...")
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
            
            # 2. Fetch all events
            self.logger.info("\n📈 Phase 2: Fetching Events...")
            events = self.events_manager.fetch_all_events(closed=False)
            self.logger.info(f"✅ Active events fetched: {len(events)}")
            
            if Config.FETCH_CLOSED_EVENTS:
                closed_events = self.events_manager.fetch_all_events(closed=True)
                self.logger.info(f"✅ Closed events fetched: {len(closed_events)}")
                events.extend(closed_events)
            
            # 3. Fetch markets for all events
            self.logger.info("\n💹 Phase 3: Fetching Markets...")
            markets = self.markets_manager.fetch_all_markets_from_events(events)
            self.logger.info(f"✅ Markets fetched: {len(markets)}")
            
            # 4. Fetch series if enabled
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Phase 4: Fetching Series...")
                series = self.series_manager.fetch_all_series()
                self.logger.info(f"✅ Series fetched: {len(series)}")
            
            # 5. Process detailed information for all entities
            self.logger.info("\n🔍 Phase 5: Fetching Detailed Information...")
            
            # Process events in detail
            self.logger.info("Processing events for detailed information...")
            self.events_manager.process_all_events_detailed()
            
            # Process markets in detail
            self.logger.info("Processing markets for detailed information...")
            self.markets_manager.process_all_markets_detailed()
            
            # Process series in detail
            if Config.FETCH_SERIES:
                self.logger.info("Processing series for detailed information...")
                self.series_manager.process_all_series_detailed()
            
            # Process tags in detail
            if Config.FETCH_TAGS:
                self.logger.info("Processing tags for detailed information...")
                self.tags_manager.process_all_tags_detailed()
            
            # 6. Fetch global metrics
            if Config.FETCH_OPEN_INTEREST:
                self.logger.info("\n💰 Phase 6: Fetching Global Metrics...")
                global_oi = self.markets_manager.fetch_global_open_interest()
                if global_oi:
                    self.logger.info(f"✅ Global Open Interest: ${global_oi:,.2f}")
            
            elapsed_time = time.time() - start_time
            
            # Get final statistics
            stats = self.db_manager.get_statistics()
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ Initial data load complete!")
            self.logger.info(f"⏱️ Time taken: {elapsed_time:.2f} seconds")
            self.logger.info("\n📊 Database Statistics:")
            for table, count in stats.items():
                self.logger.info(f"   {table}: {count:,} records")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during initial data load: {e}")
            self.logger.error("Rolling back to backup...")
            # Could implement rollback logic here
            return False
    
    def daily_update(self):
        """
        Perform daily update scan
        This should be run via cron job or scheduler
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("Starting daily update scan...")
        self.logger.info(f"Time: {datetime.now()}")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        results = {}
        
        try:
            # Backup database before updates
            backup_path = self.db_manager.backup_database()
            self.logger.info(f"Database backed up to: {backup_path}")
            
            # Update tags
            if Config.FETCH_TAGS:
                self.logger.info("\n📌 Updating tags...")
                results['tags'] = self.tags_manager.daily_scan()
            
            # Update events
            self.logger.info("\n📈 Updating events...")
            results['events'] = self.events_manager.daily_scan()
            
            # Update markets
            self.logger.info("\n💹 Updating markets...")
            results['markets'] = self.markets_manager.daily_scan()
            
            # Update series
            if Config.FETCH_SERIES:
                self.logger.info("\n📚 Updating series...")
                results['series'] = self.series_manager.daily_scan()
            
            elapsed_time = time.time() - start_time
            
            # Get final statistics
            stats = self.db_manager.get_statistics()
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ Daily update complete!")
            self.logger.info(f"⏱️ Time taken: {elapsed_time:.2f} seconds")
            self.logger.info("\n📊 Update Results:")
            for entity, count in results.items():
                self.logger.info(f"   {entity}: {count} updated")
            self.logger.info("\n📊 Database Statistics:")
            for table, count in stats.items():
                self.logger.info(f"   {table}: {count:,} records")
            self.logger.info("=" * 60)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error during daily update: {e}")
            return None
    
    def start_scheduler(self):
        """
        Start the scheduler for daily updates
        Runs in a separate thread
        """
        if not Config.ENABLE_DAILY_SCAN:
            self.logger.info("Daily scanning is disabled")
            return
        
        # Schedule daily update
        schedule.every().day.at(Config.DAILY_SCAN_TIME).do(self.daily_update)
        
        self.logger.info(f"Scheduler started. Daily scan scheduled at {Config.DAILY_SCAN_TIME}")
        
        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
    
    def start_websocket(self):
        """
        Start WebSocket connection for real-time updates
        To be implemented for live data streaming
        """
        self.logger.info("WebSocket functionality not yet implemented")
        # TODO: Implement WebSocket connection for real-time updates
        # This will connect to Polymarket's WebSocket API
        # and update the database with live data
        pass
    
    def reset_database(self):
        """
        Reset the entire database (clear all data but keep schema)
        """
        self.logger.warning("Resetting database...")
        self.db_manager.reset_database()
        self.logger.info("Database reset complete")
    
    def reset_table(self, table_name: str):
        """
        Reset a specific table
        """
        self.logger.warning(f"Resetting table: {table_name}")
        self.db_manager.reset_table(table_name)
        self.logger.info(f"Table {table_name} reset complete")
    
    def get_statistics(self) -> Dict:
        """
        Get current database statistics
        """
        return self.db_manager.get_statistics()


def main():
    """
    Main entry point for the data fetcher
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Polymarket Data Fetcher')
    parser.add_argument('--initial-load', action='store_true', 
                       help='Perform initial data load')
    parser.add_argument('--daily-update', action='store_true',
                       help='Perform daily update')
    parser.add_argument('--reset-database', action='store_true',
                       help='Reset database before loading')
    parser.add_argument('--start-scheduler', action='store_true',
                       help='Start the scheduler for daily updates')
    parser.add_argument('--statistics', action='store_true',
                       help='Show database statistics')
    
    args = parser.parse_args()
    
    fetcher = PolymarketDataFetcher()
    
    if args.statistics:
        stats = fetcher.get_statistics()
        print("\n📊 Database Statistics:")
        for table, count in stats.items():
            print(f"   {table}: {count:,} records")
    
    elif args.initial_load:
        fetcher.initial_data_load(reset_database=args.reset_database)
    
    elif args.daily_update:
        fetcher.daily_update()
    
    elif args.start_scheduler:
        fetcher.start_scheduler()
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nScheduler stopped")
    
    else:
        print("No action specified. Use --help for options")


if __name__ == "__main__":
    main()