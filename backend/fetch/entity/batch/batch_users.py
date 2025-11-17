"""
Batch users
Handles batch fetching for the users
"""

import time
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ....database.database_manager import DatabaseManager

class BatchUsersManager(DatabaseManager):
    """Manager for whale user operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.data_api_url = Config.DATA_API_URL
        self.base_url = Config.GAMMA_API_URL
        self.clob_url = Config.CLOB_API_URL
        
        # Whale thresholds
        self.MIN_WALLET_VALUE = 1000  # $1000 minimum wallet value
        self.MIN_POSITION_VALUE = 250  # $250 minimum position value
        self.TOP_HOLDERS_PER_MARKET = 25  # Top 25 holders per market
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters and collections
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._whale_wallets = set()


    def fetch_user_activity_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch detailed activity for a batch of users with improved batching"""
        self.logger.info(f"Fetching activity for {len(users)} users...")
        
        # Collect all activities in memory first
        all_activities = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in smaller chunks to avoid overwhelming the API
            chunk_size = 50
            for i in range(0, len(users), chunk_size):
                chunk = users[i:i+chunk_size]
                
                futures = {
                    executor.submit(self._fetch_user_activity_api, user): user 
                    for user in chunk
                }
                
                for future in as_completed(futures):
                    try:
                        activities = future.result()
                        all_activities.extend(activities)
                    except Exception as e:
                        self.logger.debug(f"Error in activity batch: {e}")
                
                # Small delay between chunks
                time.sleep(0.5)
        
        # Bulk insert all activities
        if all_activities:
            self._bulk_insert_activities(all_activities)
        
        return {'total_activities': len(all_activities)}





    def fetch_user_values_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch portfolio values for a batch of users"""
        self.logger.info(f"Fetching portfolio values for {len(users)} users...")
        
        values_fetched = 0
        whale_portfolios = []
        all_values = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_user_value_api, user): user 
                for user in users
            }
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result['fetched']:
                        all_values.append(result)
                        values_fetched += 1
                        if result['value'] > 50000:
                            whale_portfolios.append(result)
                except Exception as e:
                    self.logger.debug(f"Error in values batch: {e}")
        
        # Bulk insert values
        if all_values:
            self._bulk_insert_values(all_values)
        
        # Display top whale portfolios
        whale_portfolios.sort(key=lambda x: x['value'], reverse=True)
        if whale_portfolios[:5]:
            self.logger.info("🐋 Top 5 Whale Portfolios:")
            for portfolio in whale_portfolios[:5]:
                self.logger.info(f"   {portfolio['wallet'][:10]}... - ${portfolio['value']:,.2f}")
        
        return {
            'values_fetched': values_fetched,
            'whale_count': len(whale_portfolios)
        }









def fetch_top_holders_for_all_markets(self) -> Dict[str, int]:
    """
    Fetch top 25 holders for ALL active markets using multithreading
    Only store users meeting whale criteria ($1000+ wallet OR $250+ position)
    Returns: {'total_markets_processed': X, 'total_whales_found': Y}
    """
    self.logger.info("🐋 Fetching top holders for all active markets with multithreading...")
    
    # Get all active markets
    markets = self.fetch_all("""
        SELECT id, condition_id, question
        FROM markets
        WHERE active = 1 AND condition_id IS NOT NULL
        ORDER BY volume DESC
    """)
    
    # Reset counters
    self._progress_counter = 0
    self._error_counter = 0
    self._whale_wallets = set()
    
    self.logger.info(f"Processing {len(markets)} markets using {self.max_workers} threads...")
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        # Submit all tasks
        future_to_market = {
            executor.submit(self._fetch_and_filter_market_holders_thread_safe, market, len(markets)): market 
            for market in markets
        }
        
        # Process completed tasks
        for future in as_completed(future_to_market):
            market = future_to_market[future]
            try:
                whale_wallets = future.result()
                with self._progress_lock:
                    self._whale_wallets.update(whale_wallets)
            except Exception as e:
                self.logger.error(f"Error processing market {market['id']}: {e}")
    
    self.logger.info(f"✅ Found {len(self._whale_wallets)} whale users across {self._progress_counter} markets")
    
    return {
        'total_markets_processed': self._progress_counter,
        'total_whales_found': len(self._whale_wallets)
    }