"""
Markets Manager for Polymarket Terminal
Orchestrates market data fetching by coordinating with other managers
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from threading import Lock
from datetime import datetime, timedelta
from backend.database.database_manager import DatabaseManager
from backend.database.entity.store_markets import StoreMarketsManager
from backend.fetch.entity.batch.batch_markets import BatchMarketsManager
from backend.fetch.entity.id.id_markets import IdMarketsManager
from backend.fetch.entity.id.id_events import IdEventsFetcher
from backend.config import Config

class MarketsManager:
    """Main manager for market workflow orchestration"""
    
    def __init__(self):
        # Initialize configuration and database
        self.config = Config
        self.db_manager = DatabaseManager()
        self.logger = self._setup_logger()
        
        # Initialize fetchers
        self.batch_fetcher = BatchMarketsManager()
        self.id_fetcher = IdMarketsManager()
        self.event_id_fetcher = IdEventsFetcher(
            config=self.config,
            base_url=self.config.GAMMA_API_URL,
            data_api_url=self.config.DATA_API_URL
        )
        
        # Initialize storage handler
        self.storage = StoreMarketsManager()
        
        # Thread safety and progress tracking
        self._lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._start_time = None
        self._total_items = 0
    
    def _setup_logger(self):
        """Setup logger for markets manager"""
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
    
    def _calculate_eta(self, current: int, total: int) -> str:
        """Calculate estimated time of arrival"""
        if current == 0 or not self._start_time:
            return "calculating..."
        
        elapsed = time.time() - self._start_time
        rate = current / elapsed
        remaining = total - current
        
        if rate > 0:
            eta_seconds = remaining / rate
            eta = datetime.now() + timedelta(seconds=eta_seconds)
            return eta.strftime("%H:%M:%S")
        return "unknown"
    
    def _show_progress(self, current: int, total: int, description: str = "Processing"):
        """Show progress bar with ETA"""
        percentage = (current / total * 100) if total > 0 else 0
        bar_length = 40
        filled = int(bar_length * current / total) if total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        
        eta = self._calculate_eta(current, total)
        
        # Clear line and print progress
        print(f'\r{description}: |{bar}| {percentage:.1f}% ({current}/{total}) ETA: {eta}', end='', flush=True)
        
        if current >= total:
            print()  # New line when complete
    
    def fetch_markets_from_stored_events(self, num_threads: int = 20) -> List[Dict]:
        """
        Fetch markets by getting events from database and calling event API
        Delegates related data storage to appropriate managers
        
        Args:
            num_threads: Number of concurrent threads for fetching
            
        Returns:
            List of all fetched markets
        """
        self.logger.info("Starting market fetch from stored events...")
        
        # Get all active events from database
        events = self.db_manager.fetch_all("""
            SELECT id, slug, title 
            FROM events 
            WHERE active = 1 AND closed = 0 
            ORDER BY volume DESC
        """)
        
        if not events:
            self.logger.warning("No active events found in database. Please load events first.")
            return []
        
        self.logger.info(f"Found {len(events)} active events. Starting market fetch...")
        
        # Initialize progress tracking
        self._progress_counter = 0
        self._error_counter = 0
        self._total_items = len(events)
        self._start_time = time.time()
        all_markets = []
        markets_count = 0
        
        # Import managers here to avoid circular imports
        from backend.series_manager import SeriesManager
        from backend.tags_manager import TagsManager
        
        series_manager = SeriesManager()
        tags_manager = TagsManager()
        
        def fetch_event_markets(event):
            """Fetch markets for a single event by calling the event API"""
            event_id = event['id']
            
            try:
                # Fetch the full event details which includes markets
                event_data = self.event_id_fetcher.fetch_event_by_id(event_id)
                
                if event_data:
                    markets = []
                    
                    # Extract and store markets
                    if 'markets' in event_data:
                        markets = event_data['markets']
                        if markets:
                            # Ensure event_id is set
                            for market in markets:
                                market['eventId'] = event_id
                            
                            # Store markets
                            with self._lock:
                                self.storage._store_markets(markets, event_id)
                    
                    # Delegate to series manager for series data
                    if 'series' in event_data and event_data['series']:
                        series_manager.store_event_series(event_id, event_data['series'])
                    
                    # Delegate to tags manager for tag data  
                    if 'tags' in event_data and event_data['tags']:
                        tags_manager.store_event_tags(event_id, event_data['tags'])
                    
                    # Store other related data through this manager
                    if 'categories' in event_data and event_data['categories']:
                        self._store_categories(event_data['categories'], event_id)
                    
                    if 'collections' in event_data and event_data['collections']:
                        self._store_collections(event_data['collections'], event_id)
                    
                    return markets
                else:
                    return []
                    
            except Exception as e:
                with self._lock:
                    self._error_counter += 1
                self.logger.error(f"Error fetching markets for event {event_id}: {e}")
                return []
        
        # Process events concurrently with progress tracking
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {
                executor.submit(fetch_event_markets, event): event 
                for event in events
            }
            
            for future in as_completed(futures):
                try:
                    markets = future.result()
                    if markets:
                        all_markets.extend(markets)
                        markets_count += len(markets)
                    
                    with self._lock:
                        self._progress_counter += 1
                        
                        # Update progress every event
                        self._show_progress(
                            self._progress_counter, 
                            self._total_items,
                            f"Fetching markets from events"
                        )
                        
                        # Log summary every 100 events
                        if self._progress_counter % 100 == 0:
                            elapsed = time.time() - self._start_time
                            rate = self._progress_counter / elapsed
                            self.logger.info(
                                f"Progress: {self._progress_counter}/{self._total_items} events "
                                f"({markets_count} markets found, {rate:.1f} events/sec)"
                            )
                            
                except Exception as e:
                    self.logger.error(f"Error in thread: {e}")
        
        # Final statistics
        elapsed = time.time() - self._start_time
        self.logger.info(f"\n✅ Market fetch complete!")
        self.logger.info(f"   Events processed: {self._progress_counter}")
        self.logger.info(f"   Markets found: {len(all_markets)}")
        self.logger.info(f"   Errors: {self._error_counter}")
        self.logger.info(f"   Time taken: {elapsed:.1f} seconds")
        self.logger.info(f"   Average: {self._progress_counter/elapsed:.1f} events/sec")
        
        return all_markets
    
    def _store_categories(self, categories: List[Dict], event_id: str):
        """Store categories (minimal implementation, can be moved to separate manager)"""
        category_records = []
        event_category_records = []
        
        for cat in categories:
            if cat.get('id'):
                category_records.append({
                    'id': cat.get('id'),
                    'label': cat.get('label'),
                    'parent_category': cat.get('parentCategory'),
                    'slug': cat.get('slug'),
                    'published_at': cat.get('publishedAt'),
                    'created_by': cat.get('createdBy'),
                    'updated_by': cat.get('updatedBy'),
                    'created_at': cat.get('createdAt'),
                    'updated_at': cat.get('updatedAt')
                })
                
                event_category_records.append({
                    'event_id': event_id,
                    'category_id': cat.get('id')
                })
        
        if category_records:
            self.db_manager.bulk_insert_or_ignore('categories', category_records)
        if event_category_records:
            self.db_manager.bulk_insert_or_ignore('event_categories', event_category_records)
    
    def _store_collections(self, collections: List[Dict], event_id: str):
        """Store collections (minimal implementation, can be moved to separate manager)"""
        collection_records = []
        event_collection_records = []
        
        for col in collections:
            if col.get('id'):
                collection_records.append({
                    'id': col.get('id'),
                    'ticker': col.get('ticker'),
                    'slug': col.get('slug'),
                    'title': col.get('title'),
                    'subtitle': col.get('subtitle'),
                    'collection_type': col.get('collectionType'),
                    'description': col.get('description'),
                    'tags': col.get('tags'),
                    'image': col.get('image'),
                    'icon': col.get('icon'),
                    'header_image': col.get('headerImage'),
                    'layout': col.get('layout'),
                    'active': col.get('active'),
                    'closed': col.get('closed'),
                    'archived': col.get('archived'),
                    'new': col.get('new'),
                    'featured': col.get('featured'),
                    'restricted': col.get('restricted'),
                    'is_template': col.get('isTemplate'),
                    'template_variables': col.get('templateVariables'),
                    'published_at': col.get('publishedAt'),
                    'created_by': col.get('createdBy'),
                    'updated_by': col.get('updatedBy'),
                    'created_at': col.get('createdAt'),
                    'updated_at': col.get('updatedAt'),
                    'comments_enabled': col.get('commentsEnabled')
                })
                
                event_collection_records.append({
                    'event_id': event_id,
                    'collection_id': col.get('id')
                })
        
        if collection_records:
            self.db_manager.bulk_insert_or_ignore('collections', collection_records)
        if event_collection_records:
            self.db_manager.bulk_insert_or_ignore('event_collections', event_collection_records)
    
    def fetch_market_by_id(self, market_id: str) -> Optional[Dict]:
        """Fetch detailed information for a specific market"""
        return self.id_fetcher.fetch_market_by_id(market_id)
    
    def process_all_markets_detailed(self, num_threads: int = 20):
        """Process all markets to fetch detailed information with progress tracking"""
        markets = self.db_manager.fetch_all("SELECT id, question FROM markets ORDER BY volume_num DESC")
        
        if not markets:
            self.logger.warning("No markets found in database to process")
            return
        
        self.logger.info(f"Processing {len(markets)} markets for detailed information...")
        
        # Initialize progress tracking
        self._progress_counter = 0
        self._error_counter = 0
        self._total_items = len(markets)
        self._start_time = time.time()
        
        def process_market(market):
            try:
                self.fetch_market_by_id(market['id'])
                return True
            except Exception as e:
                self.logger.error(f"Error processing market {market['id']}: {e}")
                return False
        
        # Process markets concurrently with progress tracking
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {executor.submit(process_market, market): market for market in markets}
            
            for future in as_completed(futures):
                if future.result():
                    with self._lock:
                        self._progress_counter += 1
                else:
                    with self._lock:
                        self._error_counter += 1
                
                # Update progress
                with self._lock:
                    self._show_progress(
                        self._progress_counter + self._error_counter,
                        self._total_items,
                        "Processing market details"
                    )
                    
                    # Log every 100 markets
                    if (self._progress_counter + self._error_counter) % 100 == 0:
                        elapsed = time.time() - self._start_time
                        rate = self._progress_counter / elapsed if elapsed > 0 else 0
                        self.logger.info(
                            f"Detailed processing: {self._progress_counter} successful, "
                            f"{self._error_counter} errors ({rate:.1f} markets/sec)"
                        )
        
        self.logger.info(f"\n✅ Market processing complete. Processed: {self._progress_counter}, Errors: {self._error_counter}")
    
    def load_markets_only(self, event_ids: List[str] = None) -> Dict:
        """
        Load only markets data - callable from data_fetcher
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 Loading MARKETS from stored events")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            markets = self.fetch_markets_from_stored_events(num_threads=20)
            result['count'] = len(markets)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Markets loaded: {result['count']}")
            self.logger.info(f"⏱️ Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading markets: {e}")
            
        return result
    
    def delete_markets_only(self, keep_active: bool = False) -> Dict:
        """Delete markets data"""
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"🗑️ Deleting {'CLOSED' if keep_active else 'ALL'} MARKETS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            if keep_active:
                before_count = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM markets WHERE closed = 1")
                deleted = self.db_manager.delete_records('markets', 'closed = 1', commit=True)
                result['deleted'] = before_count['count'] if before_count else 0
            else:
                before_count = self.db_manager.get_table_count('markets')
                
                # Delete all markets and cascade
                self.db_manager.delete_records('market_tags', commit=False)
                self.db_manager.delete_records('market_categories', commit=False)
                self.db_manager.delete_records('market_open_interest', commit=False)
                self.db_manager.delete_records('market_holders', commit=False)
                deleted = self.db_manager.delete_records('markets', commit=True)
                result['deleted'] = before_count
            
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} markets and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting markets: {e}")
            
        return result
    
    def get_market_statistics(self) -> Dict:
        """Get statistics about markets in the database"""
        stats = {}
        
        # Total markets
        total = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM markets")
        stats['total_markets'] = total['count'] if total else 0
        
        # Active markets
        active = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM markets WHERE active = 1")
        stats['active_markets'] = active['count'] if active else 0
        
        # Closed markets
        closed = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM markets WHERE closed = 1")
        stats['closed_markets'] = closed['count'] if closed else 0
        
        return stats
    
    def close_connection(self):
        """Close database connection"""
        self.db_manager.close_connection()
        if hasattr(self.storage, 'close_connection'):
            self.storage.close_connection()
        if hasattr(self.batch_fetcher, 'close_connection'):
            self.batch_fetcher.close_connection()
        if hasattr(self.id_fetcher, 'close_connection'):
            self.id_fetcher.close_connection()
    
    def __del__(self):
        """Cleanup on deletion"""
        self.close_connection()