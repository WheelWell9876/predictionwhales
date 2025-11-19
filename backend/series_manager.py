"""
Series Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing series data with concurrent requests
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
from backend.fetch.entity.batch.batch_series import BatchSeriesManager
from backend.fetch.entity.id.id_series import IdSeriesManager
from backend.database.entity.store_series import StoreSeriesManager

class SeriesManager:
    """Manager for series-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchSeriesManager()
        self.id_manager = IdSeriesManager()
        self.store_manager = StoreSeriesManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()

    def fetch_all_series(self, limit: int = 100) -> List[Dict]:
        """
        Fetch all series from the API
        
        Args:
            limit: Number of series per request
        """
        return self.batch_manager.fetch_all_series(limit)

    def fetch_series_by_id(self, series_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific series
        """
        return self.id_manager.fetch_series_by_id(series_id)

    def fetch_series_by_id_parallel(self, series_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific series with parallel sub-requests
        """
        return self.id_manager.fetch_series_by_id_parallel(series_id)

    def process_all_series_detailed(self, use_parallel: bool = True, num_threads: int = 20):
        """
        Process all series to fetch detailed information with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests
            num_threads: Number of concurrent threads (default: 20)
        """
        # Get all series IDs from database
        series_list = self.db_manager.fetch_all("SELECT id, slug FROM series ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(series_list)} series for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        lock = Lock()
        
        # Choose which fetch method to use
        fetch_method = self.fetch_series_by_id_parallel if use_parallel else self.fetch_series_by_id
        
        def process_series(series):
            nonlocal processed, errors
            try:
                fetch_method(series['id'])
                with lock:
                    processed += 1
                    if processed % 10 == 0:
                        self.logger.info(f"Processed {processed}/{len(series_list)} series")
            except Exception as e:
                with lock:
                    errors += 1
                self.logger.error(f"Error processing series {series['id']}: {e}")
        
        # Process series concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_series, series_list)
        
        self.logger.info(f"✅ Series processing complete. Processed: {processed}, Errors: {errors}")

    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for series updates
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        if not self.config.FETCH_SERIES:
            self.logger.info("Series fetching disabled")
            return 0
        
        self.logger.info("Starting daily series scan...")
        
        # Fetch all series
        all_series = self.fetch_all_series()
        
        # Process detailed information
        self.process_all_series_detailed(use_parallel=use_parallel)
        
        self.logger.info(f"Daily series scan complete. Total series: {len(all_series)}")
        
        return len(all_series)

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all series manager database connections...")
        
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

    def delete_series_only(self) -> Dict:
        """
        Delete series data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting SERIES Data")
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
                cursor.execute("SELECT COUNT(*) FROM series")
                before_count = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all related data
                tables_to_clear = [
                    'series_events',
                    'series_collections',
                    'series_tags',
                    'series'
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
            self.logger.info(f"✅ Deleted {result['deleted']} series and related data")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting series: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_series_only(self) -> Dict:
        """
        Load only series data
        
        Returns:
            Dict with success status, count of series loaded, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📚 Loading SERIES Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}
        
        try:
            series = self.fetch_all_series()
            result['count'] = len(series)
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Series loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading series: {e}")
            
        return result