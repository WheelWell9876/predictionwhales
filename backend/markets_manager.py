"""
Markets Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing market data with concurrent requests
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
from backend.fetch.entity.batch.batch_markets import BatchMarketsManager
from backend.fetch.entity.id.id_markets import IdMarketsManager
from backend.database.entity.store_markets import StoreMarketsManager

class MarketsManager:
    """Manager for market-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchMarketsManager()
        self.id_manager = IdMarketsManager()
        self.store_manager = StoreMarketsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()

    def fetch_all_markets_from_events(self, events: List[Dict]) -> List[Dict]:
        """
        Fetch all markets from a list of events with multithreading
        
        Args:
            events: List of event dictionaries
        """
        return self.batch_manager.fetch_all_markets_from_events(events)

    def fetch_market_by_id(self, market_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific market
        """
        return self.id_manager.fetch_market_by_id(market_id)

    def process_all_markets_detailed(self, num_threads: int = 20):
        """
        Process all markets to fetch detailed information with multithreading
        
        Args:
            num_threads: Number of concurrent threads (default: 20)
        """
        # Get all market IDs from database
        markets = self.db_manager.fetch_all("SELECT id, question FROM markets ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(markets)} markets for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        lock = Lock()
        
        def process_market(market):
            nonlocal processed, errors
            try:
                self.fetch_market_by_id(market['id'])
                with lock:
                    processed += 1
                    if processed % 100 == 0:
                        self.logger.info(f"Processed {processed}/{len(markets)} markets")
            except Exception as e:
                with lock:
                    errors += 1
                self.logger.error(f"Error processing market {market['id']}: {e}")
        
        # Process markets concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_market, markets)
        
        self.logger.info(f"✅ Market processing complete. Processed: {processed}, Errors: {errors}")

    def fetch_market_open_interest(self, market_id: str, condition_id: str):
        """
        Fetch open interest data for a market (placeholder for future implementation)
        """
        # This would be implemented based on your open interest API endpoint
        pass

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all market manager database connections...")
        
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

    def delete_markets_only(self) -> Dict:
        """
        Delete markets data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting MARKETS Data")
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
                cursor.execute("SELECT COUNT(*) FROM markets")
                before_count = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all related data
                tables_to_clear = [
                    'market_tags',
                    'market_open_interest',
                    'market_holders',
                    'markets'
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
            self.logger.info(f"✅ Deleted {result['deleted']} markets and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting markets: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_markets_only(self, event_ids: List[str] = None) -> Dict:
        """
        Load only markets data
        
        Args:
            event_ids: Optional list of event IDs to fetch markets for
        
        Returns:
            Dict with success status, count of markets loaded, and any error
        """
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
                    self.logger.warning("⚠️  No active events found. Please load events first.")
                    result['error'] = "No events available"
                    return result
            
            markets = self.fetch_all_markets_from_events(events)
            result['count'] = len(markets)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Markets loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading markets: {e}")
            
        return result