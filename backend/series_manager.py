"""
Series Manager for Polymarket Terminal
Handles fetching, processing, and storing series data with multithreading support
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config

class SeriesManager(DatabaseManager):
    """Manager for series-related operations with multithreading support"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
    





    
    def process_all_series_detailed(self, use_parallel: bool = True):
        """
        Process all series to fetch detailed information with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests (events, collections)
        """
        # Get all series IDs from database
        series_list = self.fetch_all("SELECT id, slug FROM series ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(series_list)} series for detailed information using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Choose which fetch method to use
            fetch_method = self.fetch_series_by_id_parallel if use_parallel else self.fetch_series_by_id
            
            # Submit all tasks
            future_to_series = {
                executor.submit(self._process_series_detailed, series, fetch_method, len(series_list)): series 
                for series in series_list
            }
            
            # Process completed tasks
            for future in as_completed(future_to_series):
                series = future_to_series[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing series {series['id']}: {e}")
        
        self.logger.info(f"Series processing complete. Processed: {self._progress_counter}, Errors: {self._error_counter}")
    
    def _process_series_detailed(self, series: Dict, fetch_method, total_series: int):
        """
        Helper method to process a single series
        Thread-safe wrapper for parallel execution
        """
        try:
            fetch_method(series['id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0:
                    self.logger.info(f"Processed {self._progress_counter}/{total_series} series")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e
    
    def batch_fetch_series(self, series_ids: List[str], use_parallel: bool = True) -> List[Dict]:
        """
        Fetch multiple series in parallel
        
        Args:
            series_ids: List of series IDs to fetch
            use_parallel: If True, uses parallel fetching for sub-requests
            
        Returns:
            List of series dictionaries
        """
        self.logger.info(f"Batch fetching {len(series_ids)} series using {self.max_workers} threads...")
        
        series_results = []
        fetch_method = self.fetch_series_by_id_parallel if use_parallel else self.fetch_series_by_id
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(fetch_method, series_id): series_id 
                for series_id in series_ids
            }
            
            for future in as_completed(future_to_id):
                series_id = future_to_id[future]
                try:
                    series = future.result()
                    if series:
                        series_results.append(series)
                except Exception as e:
                    self.logger.error(f"Error fetching series {series_id}: {e}")
        
        return series_results
    
    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for series updates with multithreading
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        if not self.config.FETCH_SERIES:
            self.logger.info("Series fetching disabled")
            return 0
        
        self.logger.info("Starting daily series scan with multithreading...")
        
        # Fetch all series
        all_series = self.fetch_all_series()
        
        # Process detailed information (parallelized)
        self.process_all_series_detailed(use_parallel=use_parallel)
        
        self.logger.info(f"Daily series scan complete. Total series: {len(all_series)}")
        
        return len(all_series)