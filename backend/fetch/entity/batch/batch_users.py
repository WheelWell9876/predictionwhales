"""
Batch users
Handles batch fetching for the users
"""

import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import requests
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_users import StoreUsersManager

class BatchUsersManager(DatabaseManager):
    """Manager for batch user fetching"""

    def __init__(self):
        super().__init__()
        self.config = Config
        self.data_api_url = Config.DATA_API_URL
        self.base_url = Config.GAMMA_API_URL
        self.clob_url = Config.CLOB_API_URL
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StoreUsersManager()
        
        # Whale thresholds
        self.MIN_WALLET_VALUE = 1000  # $1000 minimum wallet value
        self.MIN_POSITION_VALUE = 250  # $250 minimum position value
        self.TOP_HOLDERS_PER_MARKET = 25  # Top 25 holders per market
        self.MIN_TRANSACTION_SIZE = Config.MIN_TRANSACTION_SIZE if hasattr(Config, 'MIN_TRANSACTION_SIZE') else 500
        
        # Set max workers
        self.max_workers = min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe counters and collections
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._whale_wallets = set()

    def fetch_top_holders_for_all_markets(self) -> Dict[str, int]:
        """
        Fetch top 25 holders for ALL active markets using multithreading
        Only store users meeting whale criteria ($1000+ wallet OR $250+ position)
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

    def _fetch_and_filter_market_holders_thread_safe(self, market: Dict, total_markets: int) -> Set[str]:
        """Thread-safe wrapper for fetching and filtering market holders"""
        try:
            whale_wallets = self._fetch_and_filter_market_holders(market['id'], market['condition_id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0:
                    self.logger.info(f"  Processed {self._progress_counter}/{total_markets} markets, found {len(self._whale_wallets)} unique whales")
            
            # Rate limiting
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
            return whale_wallets
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e

    def _fetch_and_filter_market_holders(self, market_id: str, condition_id: str) -> Set[str]:
        """Fetch top holders for a specific market and filter by whale criteria"""
        try:
            # Fetch market holders using DATA API
            url = f"{self.data_api_url}/holders"
            params = {
                "market": condition_id,
                "minBalance": 1,
                "limit": 100
            }
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                return set()
            
            holders_data = response.json()
            
            if not holders_data:
                return set()
            
            whale_wallets = set()
            holder_records = []
            
            # Process all token groups (YES/NO outcomes)
            for token_group in holders_data:
                token_id = token_group.get('token')
                holders = token_group.get('holders', [])
                outcome_index = token_group.get('outcomeIndex', 0)
                
                # Process each holder
                for holder in holders[:self.TOP_HOLDERS_PER_MARKET]:
                    proxy_wallet = holder.get('proxyWallet')
                    if not proxy_wallet:
                        continue
                    
                    # Check if meets whale criteria
                    is_whale, user_data = self._check_whale_criteria_from_holder(holder, market_id)
                    
                    if is_whale:
                        whale_wallets.add(proxy_wallet)
                        
                        # Store user data
                        if user_data:
                            with self._lock:
                                self.store_manager._store_user(user_data)
                        
                        # Store holder record
                        holder_record = {
                            'market_id': market_id,
                            'token_id': token_id,
                            'proxy_wallet': proxy_wallet,
                            'username': holder.get('name'),
                            'pseudonym': holder.get('pseudonym'),
                            'amount': holder.get('amount', 0),
                            'outcome_index': outcome_index,
                            'bio': holder.get('bio'),
                            'profile_image': holder.get('profileImage'),
                            'updated_at': datetime.now().isoformat()
                        }
                        holder_records.append(holder_record)
            
            # Bulk insert holders
            if holder_records:
                with self._lock:
                    self.bulk_insert_or_replace('market_holders', holder_records)
            
            return whale_wallets
            
        except Exception as e:
            self.logger.error(f"Error fetching holders for market {market_id}: {e}")
            return set()

    def _check_whale_criteria_from_holder(self, holder: Dict, market_id: str) -> tuple[bool, Optional[Dict]]:
        """Check if holder meets whale criteria"""
        proxy_wallet = holder.get('proxyWallet')
        
        # Get wallet value
        wallet_value = self._fetch_user_wallet_value(proxy_wallet)
        
        # Check position value
        position_shares = holder.get('amount', 0)
        estimated_position_value = position_shares * 0.5  # Conservative estimate
        
        # Check whale criteria
        is_whale = (
            wallet_value >= self.MIN_WALLET_VALUE or 
            estimated_position_value >= self.MIN_POSITION_VALUE or
            (wallet_value >= 500 and position_shares >= 100)
        )
        
        if not is_whale:
            return False, None
        
        # Prepare user record
        user_data = {
            'proxy_wallet': proxy_wallet,
            'username': holder.get('name'),
            'pseudonym': holder.get('pseudonym'),
            'bio': holder.get('bio'),
            'profile_image': holder.get('profileImage'),
            'profile_image_optimized': holder.get('profileImageOptimized'),
            'total_value': wallet_value,
            'is_whale': 1,
            'last_updated': datetime.now().isoformat(),
            'created_at': datetime.now().isoformat()
        }
        
        return True, user_data

    def _fetch_user_wallet_value(self, proxy_wallet: str) -> float:
        """Fetch user's total wallet value"""
        try:
            url = f"{self.data_api_url}/portfolio-value"
            params = {"user": proxy_wallet}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('totalValue', 0) if data else 0
            
            return 0
            
        except Exception as e:
            return 0

    def fetch_user_activity_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch detailed activity for a batch of users"""
        self.logger.info(f"Fetching activity for {len(users)} users...")
        
        all_activities = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
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
                
                time.sleep(0.5)
        
        # Bulk insert all activities
        if all_activities:
            self.store_manager._bulk_insert_activities(all_activities)
        
        return {'total_activities': len(all_activities)}

    def _fetch_user_activity_api(self, proxy_wallet: str) -> List[Dict]:
        """Fetch activity for a single user from API"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {
                "user": proxy_wallet,
                "limit": 500,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return []
            
            activities = response.json()
            
            if not activities:
                return []
            
            # Filter and prepare activities
            filtered_activities = []
            
            for activity in activities:
                # Only store significant activities
                usdc_size = activity.get('usdcSize', 0)
                if usdc_size < self.MIN_TRANSACTION_SIZE:
                    continue
                
                # Add proxy_wallet to activity
                activity['proxyWallet'] = proxy_wallet
                filtered_activities.append(activity)
            
            return filtered_activities
            
        except Exception as e:
            self.logger.debug(f"Error fetching activity for {proxy_wallet}: {e}")
            return []

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
            self.store_manager._bulk_insert_values(all_values)
        
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

    def _fetch_user_value_api(self, proxy_wallet: str) -> Dict:
        """Fetch portfolio value for a single user from API"""
        try:
            url = f"{self.data_api_url}/value"
            params = {"user": proxy_wallet}
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}
            
            value_data = response.json()
            
            if not value_data or len(value_data) == 0:
                return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}
            
            total_value = value_data[0].get('value', 0)
            
            return {'fetched': True, 'wallet': proxy_wallet, 'value': total_value}
            
        except Exception as e:
            self.logger.debug(f"Error fetching value for {proxy_wallet}: {e}")
            return {'fetched': False, 'wallet': proxy_wallet, 'value': 0}