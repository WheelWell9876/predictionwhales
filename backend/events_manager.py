"""
Events Manager for Polymarket Terminal
Main facilitator for event workflow and data flow
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from backend.database.database_manager import DatabaseManager
from backend.database.entity.store_events import StoreEvents
from backend.fetch.entity.batch.batch_events import BatchEventsFetcher
from backend.fetch.entity.id.id_events import IdEventsFetcher
from backend.config import Config

class EventsManager:
    """Main manager for event workflow orchestration"""
    
    def __init__(self):
        # Initialize configuration and database
        self.config = Config
        self.db_manager = DatabaseManager()
        self.logger = self._setup_logger()
        
        # Initialize fetchers
        self.batch_fetcher = BatchEventsFetcher(
            config=self.config,
            base_url=self.config.GAMMA_API_URL
        )
        self.id_fetcher = IdEventsFetcher(
            config=self.config,
            base_url=self.config.GAMMA_API_URL,
            data_api_url=self.config.DATA_API_URL
        )
        
        # Initialize storage handler
        self.storage = StoreEvents(self.db_manager)
    
    def _setup_logger(self):
        """Setup logger for events manager"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            fh = logging.FileHandler(self.config.LOG_FILE)
            fh.setLevel(logging.DEBUG)
            
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            fh.setFormatter(formatter)
            
            logger.addHandler(ch)
            logger.addHandler(fh)
        
        return logger
    
    def fetch_all_events(self, closed: bool = False, limit: int = 100, num_threads: int = 5) -> List[Dict]:
        """
        Fetch all events and store them in the database
        
        Args:
            closed: Whether to fetch closed events (always False for active events only)
            limit: Events per batch request
            num_threads: Number of concurrent threads
            
        Returns:
            List of all fetched events
        """
        self.logger.info(f"Starting event fetch (threads={num_threads})...")
        
        # Fetch all events using batch fetcher
        events = self.batch_fetcher.fetch_all_events(limit=limit, num_threads=num_threads)
        
        if events:
            # Store events in database
            self.storage.store_events_batch(events)
            self.logger.info(f"Successfully fetched and stored {len(events)} events")
        else:
            self.logger.warning("No events fetched")
        
        return events
    
    def fetch_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific event
        
        Args:
            event_id: The event ID to fetch
            
        Returns:
            Event dictionary or None if failed
        """
        # Fetch detailed event data
        event = self.id_fetcher.fetch_event_by_id(event_id)
        
        if event:
            # Store detailed event
            self.storage.store_event_detailed(event)
            
            # Fetch and store additional data if configured
            if self.config.FETCH_TAGS:
                tags = self.id_fetcher.fetch_event_tags(event_id)
                if tags:
                    self.storage.store_event_tags(event_id, tags)
            
            if self.config.FETCH_LIVE_VOLUME:
                volume_data = self.id_fetcher.fetch_event_live_volume(event_id)
                if volume_data:
                    self.storage.store_event_live_volume(event_id, volume_data)
        
        return event
    
    def process_all_events_detailed(self, num_threads: int = 10):
        """
        Process all events to fetch detailed information with multithreading
        
        Args:
            num_threads: Number of concurrent threads
        """
        # Get all event IDs from database
        events = self.db_manager.fetch_all("SELECT id, slug FROM events ORDER BY volume DESC")
        
        if not events:
            self.logger.warning("No events found in database to process")
            return
        
        self.logger.info(f"Processing {len(events)} events for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        
        def process_event(event):
            try:
                self.fetch_event_by_id(event['id'])
                return True
            except Exception as e:
                self.logger.error(f"Error processing event {event['id']}: {e}")
                return False
        
        # Process events concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(process_event, event): event for event in events}
            
            for future in as_completed(futures):
                if future.result():
                    processed += 1
                else:
                    errors += 1
                
                if processed % 50 == 0:
                    self.logger.info(f"Processed {processed}/{len(events)} events")
        
        self.logger.info(f"Event processing complete. Processed: {processed}, Errors: {errors}")
    
    def load_events_only(self, closed: bool = False) -> Dict:
        """
        Load only events data - callable from data_fetcher
        
        Args:
            closed: Whether to include closed events
            
        Returns:
            Dictionary with load results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"📅 Loading {'ALL' if closed else 'ACTIVE'} EVENTS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            events = self.fetch_all_events(closed=closed)
            result['count'] = len(events)
            result['success'] = True
            
            # Clean up closed events if not fetching them
            if not closed:
                self.logger.info("🧹 Cleaning up closed events...")
                self.remove_closed_events()
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Events loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading events: {e}")
            
        return result
    
    def delete_events_only(self, keep_active: bool = True) -> Dict:
        """
        Delete events data - callable from data_fetcher
        
        Args:
            keep_active: If True, only delete closed events. If False, delete all events
            
        Returns:
            Dictionary with deletion results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"🗑️  Deleting {'CLOSED' if keep_active else 'ALL'} EVENTS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            if keep_active:
                # Delete only closed/inactive events
                deleted = self.remove_closed_events()
                result['deleted'] = deleted
            else:
                # Get current count
                before_count = self.db_manager.get_table_count('events')
                
                # Delete all events and cascade
                self.db_manager.delete_records('event_tags', commit=False)
                self.db_manager.delete_records('series_events', commit=False)
                self.db_manager.delete_records('event_live_volume', commit=False)
                self.db_manager.delete_records('comments', commit=False)
                self.db_manager.delete_records('markets', commit=False)
                deleted = self.db_manager.delete_records('events', commit=True)
                result['deleted'] = before_count
            
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} events and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting events: {e}")
            
        return result
    
    def daily_scan(self) -> int:
        """
        Perform daily scan for new events
        Only fetches active events and processes their details
        
        Returns:
            Number of active events found
        """
        self.logger.info("Starting daily event scan...")
        
        # Fetch only active events
        active_events = self.fetch_all_events(closed=False, num_threads=5)
        
        if active_events and self.config.FETCH_DETAILED_INFO:
            # Process detailed information for events
            self.process_all_events_detailed(num_threads=10)
        
        self.logger.info(f"Daily event scan complete. Found {len(active_events)} active events")
        
        return len(active_events)
    
    def remove_closed_events(self) -> int:
        """
        Remove all closed events from the database
        
        Returns:
            Number of events removed
        """
        return self.db_manager.remove_closed_events()
    
    def get_active_events(self) -> List[Dict]:
        """
        Get all active events from the database
        
        Returns:
            List of active event dictionaries
        """
        return self.db_manager.fetch_all("""
            SELECT id, slug, title, volume, liquidity 
            FROM events 
            WHERE active = 1 AND closed = 0 
            ORDER BY volume DESC
        """)
    
    def get_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        Get an event from the database by ID
        
        Args:
            event_id: The event ID
            
        Returns:
            Event dictionary or None if not found
        """
        return self.db_manager.fetch_one(
            "SELECT * FROM events WHERE id = ?",
            (event_id,)
        )
    
    def get_event_statistics(self) -> Dict:
        """
        Get statistics about events in the database
        
        Returns:
            Dictionary with event statistics
        """
        stats = {}
        
        # Total events
        total = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM events")
        stats['total_events'] = total['count'] if total else 0
        
        # Active events
        active = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM events WHERE active = 1")
        stats['active_events'] = active['count'] if active else 0
        
        # Closed events
        closed = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
        stats['closed_events'] = closed['count'] if closed else 0
        
        # Total volume
        volume = self.db_manager.fetch_one("SELECT SUM(volume) as total FROM events WHERE active = 1")
        stats['total_volume'] = volume['total'] if volume and volume['total'] else 0
        
        # Total liquidity
        liquidity = self.db_manager.fetch_one("SELECT SUM(liquidity) as total FROM events WHERE active = 1")
        stats['total_liquidity'] = liquidity['total'] if liquidity and liquidity['total'] else 0
        
        return stats
    
    def close_connection(self):
        """Close database connection"""
        self.db_manager.close_connection()
    
    def __del__(self):
        """Cleanup on deletion"""
        self.close_connection()