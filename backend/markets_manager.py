"""
Markets Manager for Polymarket Terminal
Handles fetching, processing, and storing market data with multithreading support
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database_manager import DatabaseManager
from .config import Config

class MarketsManager(DatabaseManager):
    """Manager for market-related operations with multithreading support"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
    
    def fetch_all_markets_from_events(self, events: List[Dict]) -> List[Dict]:
        """
        Fetch all markets from a list of events using multithreading
        Used after fetching events to get their markets
        """
        all_markets = []
        total_events = len(events)
        
        self.logger.info(f"Fetching markets for {total_events} events using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_event = {
                executor.submit(self._fetch_and_store_event_markets, event, idx, total_events): event 
                for idx, event in enumerate(events, 1)
            }
            
            # Process completed tasks
            for future in as_completed(future_to_event):
                event = future_to_event[future]
                try:
                    markets = future.result()
                    if markets:
                        all_markets.extend(markets)
                except Exception as e:
                    event_id = event.get('id') if isinstance(event, dict) else event
                    self.logger.error(f"Error in thread processing event {event_id}: {e}")
        
        self.logger.info(f"Total markets fetched: {len(all_markets)}")
        self.logger.info(f"Errors encountered: {self._error_counter}")
        return all_markets
    
    def _fetch_and_store_event_markets(self, event: Dict, idx: int, total_events: int) -> List[Dict]:
        """
        Helper method to fetch and store markets for a single event
        Thread-safe wrapper for parallel execution
        """
        event_id = event.get('id') if isinstance(event, dict) else event
        
        try:
            # Fetch markets for this event
            markets = self._fetch_markets_for_event(event_id)
            
            if markets:
                # Store markets (thread-safe)
                with self._db_lock:
                    self._store_markets(markets, event_id)
            
            # Update progress (thread-safe)
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0 or self._progress_counter == total_events:
                    self.logger.info(f"Progress: {self._progress_counter}/{total_events} events processed")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
            return markets
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            self.logger.error(f"Error fetching markets for event {event_id}: {e}")
            return []
    
    def fetch_market_by_id(self, market_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific market
        """
        try:
            url = f"{self.base_url}/markets/{market_id}"
            params = {"include_tag": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            market = response.json()
            
            # Store the detailed market
            self._store_market_detailed(market)
            
            # Fetch and store tags
            if 'tags' in market:
                self._store_market_tags(market_id, market['tags'])
            
            # Fetch open interest if enabled
            if self.config.FETCH_OPEN_INTEREST:
                self.fetch_market_open_interest(market_id, market.get('conditionId'))
            
            return market
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching market {market_id}: {e}")
            return None
    
    def fetch_market_by_id_parallel(self, market_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific market with parallel sub-requests
        Fetches market details, tags, and open interest concurrently
        """
        try:
            # Main market fetch
            url = f"{self.base_url}/markets/{market_id}"
            params = {"include_tag": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            market = response.json()
            
            # Parallel execution of sub-tasks
            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = []
                
                # Store market details
                futures.append(executor.submit(self._store_market_detailed, market))
                
                # Fetch tags if present
                if 'tags' in market:
                    futures.append(executor.submit(self._store_market_tags, market_id, market['tags']))
                
                # Fetch open interest if enabled
                if self.config.FETCH_OPEN_INTEREST and market.get('conditionId'):
                    futures.append(executor.submit(
                        self.fetch_market_open_interest, 
                        market_id, 
                        market.get('conditionId')
                    ))
                
                # Wait for all to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error in parallel market fetch subtask: {e}")
            
            return market
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching market {market_id}: {e}")
            return None
    
    def fetch_market_tags(self, market_id: str) -> List[Dict]:
        """
        Fetch tags for a specific market
        """
        try:
            url = f"{self.base_url}/markets/{market_id}/tags"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            
            # Store tags and relationships
            if tags:
                self._store_market_tags(market_id, tags)
            
            return tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for market {market_id}: {e}")
            return []
    
    def fetch_market_open_interest(self, market_id: str, condition_id: str = None) -> Optional[float]:
        """
        Fetch open interest data for a market
        """
        if not self.config.FETCH_OPEN_INTEREST:
            return None
        
        try:
            url = f"{self.data_api_url}/oi"
            
            # Try with market_id first
            params = {"market": market_id}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200 and condition_id:
                # Try with condition_id if market_id fails
                params = {"market": condition_id}
                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
            
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                oi_value = data[0].get('value', 0)
                with self._db_lock:
                    self._store_open_interest(market_id, condition_id, oi_value)
                return oi_value
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching open interest for market {market_id}: {e}")
            return None
    
    def _fetch_markets_for_event(self, event_id: str) -> List[Dict]:
        """
        Fetch markets for a specific event
        """
        try:
            # First try to get markets from event endpoint
            url = f"{self.base_url}/events/{event_id}"
            params = {"include_markets": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                event_data = response.json()
                return event_data.get('markets', [])
            
            # Fallback to markets endpoint with event filter
            url = f"{self.base_url}/markets"
            params = {"event_id": event_id, "limit": self.config.MAX_MARKETS_PER_EVENT}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching markets for event {event_id}: {e}")
            return []
    
    def _store_markets(self, markets: List[Dict], event_id: str = None):
        """
        Store multiple markets in the database
        Thread-safe when called with _db_lock
        """
        market_records = []
        
        for market in markets:
            market_record = self._prepare_market_record(market, event_id)
            market_records.append(market_record)
        
        if market_records:
            self.bulk_insert_or_replace('markets', market_records)
            self.logger.debug(f"Stored {len(market_records)} markets for event {event_id}")
    
    def _store_market_detailed(self, market: Dict):
        """
        Store detailed market information
        Thread-safe when called with _db_lock
        """
        market_record = self._prepare_detailed_market_record(market)
        
        with self._db_lock:
            self.insert_or_replace('markets', market_record)
            self.logger.debug(f"Stored detailed market {market.get('id')}")
    
    def _prepare_market_record(self, market: Dict, event_id: str = None) -> Dict:
        """
        Prepare a market record for database insertion
        """
        return {
            'id': market.get('id'),
            'event_id': event_id or market.get('eventId'),
            'slug': market.get('slug'),
            'question': market.get('question'),
            'condition_id': market.get('conditionId'),
            'description': market.get('description'),
            'outcomes': json.dumps(market.get('outcomes')) if market.get('outcomes') else None,
            'outcome_prices': json.dumps(market.get('outcomePrices')) if market.get('outcomePrices') else None,
            'volume': market.get('volume'),
            'volume_clob': market.get('volumeClob'),
            'liquidity': market.get('liquidity'),
            'liquidity_clob': market.get('liquidityClob'),
            'open_interest': market.get('openInterest'),
            'icon': market.get('icon'),
            'image': market.get('image'),
            'start_date': market.get('startDate'),
            'end_date': market.get('endDate'),
            'resolution_source': market.get('resolutionSource'),
            'volume_24hr': market.get('volume24hr'),
            'volume_24hr_clob': market.get('volume24hrClob'),
            'volume_1wk': market.get('volume1wk'),
            'volume_1wk_clob': market.get('volume1wkClob'),
            'volume_1mo': market.get('volume1mo'),
            'volume_1mo_clob': market.get('volume1moClob'),
            'volume_1yr': market.get('volume1yr'),
            'volume_1yr_clob': market.get('volume1yrClob'),
            'last_trade_price': market.get('lastTradePrice'),
            'best_bid': market.get('bestBid'),
            'best_ask': market.get('bestAsk'),
            'spread': market.get('spread'),
            'one_day_price_change': market.get('oneDayPriceChange'),
            'one_week_price_change': market.get('oneWeekPriceChange'),
            'one_month_price_change': market.get('oneMonthPriceChange'),
            'competitive': market.get('competitive'),
            'market_maker_address': market.get('marketMakerAddress'),
            'submitted_by': market.get('submitted_by'),
            'resolved_by': market.get('resolvedBy'),
            'question_id': market.get('questionID'),
            'group_item_title': market.get('groupItemTitle'),
            'group_item_threshold': market.get('groupItemThreshold'),
            'enable_order_book': market.get('enableOrderBook'),
            'order_price_min_tick_size': market.get('orderPriceMinTickSize'),
            'order_min_size': market.get('orderMinSize'),
            'active': market.get('active'),
            'closed': market.get('closed'),
            'archived': market.get('archived'),
            'new': market.get('new'),
            'featured': market.get('featured'),
            'restricted': market.get('restricted'),
            'ready': market.get('ready'),
            'funded': market.get('funded'),
            'neg_risk': market.get('negRisk'),
            'neg_risk_other': market.get('negRiskOther'),
            'cyom': market.get('cyom'),
            'has_reviewed_dates': market.get('hasReviewedDates'),
            'accepting_orders': market.get('acceptingOrders'),
            'accepting_orders_timestamp': market.get('acceptingOrdersTimestamp'),
            'automatically_active': market.get('automaticallyActive'),
            'clear_book_on_start': market.get('clearBookOnStart'),
            'manual_activation': market.get('manualActivation'),
            'pending_deployment': market.get('pendingDeployment'),
            'deploying': market.get('deploying'),
            'rfq_enabled': market.get('rfqEnabled'),
            'holding_rewards_enabled': market.get('holdingRewardsEnabled'),
            'fees_enabled': market.get('feesEnabled'),
            'pager_duty_notification_enabled': market.get('pagerDutyNotificationEnabled'),
            'approved': market.get('approved'),
            'rewards_min_size': market.get('rewardsMinSize'),
            'rewards_max_spread': market.get('rewardsMaxSpread'),
            'uma_bond': market.get('umaBond'),
            'uma_reward': market.get('umaReward'),
            'uma_resolution_statuses': json.dumps(market.get('umaResolutionStatuses')) if market.get('umaResolutionStatuses') else None,
            'created_at': market.get('createdAt'),
            'updated_at': market.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }
    
    def _prepare_detailed_market_record(self, market: Dict) -> Dict:
        """
        Prepare a detailed market record for database insertion
        """
        return self._prepare_market_record(market)
    
    def _store_market_tags(self, market_id: str, tags: List[Dict]):
        """
        Store tags and relationships for a market
        Thread-safe when called with _db_lock
        """
        tag_records = []
        relationships = []
        
        for tag in tags:
            tag_record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            tag_records.append(tag_record)
            
            relationship = {
                'market_id': market_id,
                'tag_id': tag.get('id')
            }
            relationships.append(relationship)
        
        # Bulk insert with lock
        if tag_records:
            with self._db_lock:
                self.bulk_insert_or_replace('tags', tag_records)
        if relationships:
            with self._db_lock:
                self.bulk_insert_or_replace('market_tags', relationships)
        
        self.logger.debug(f"Stored {len(tags)} tags for market {market_id}")
    
    def _store_open_interest(self, market_id: str, condition_id: str, oi_value: float):
        """
        Store open interest data for a market
        Thread-safe when called with _db_lock
        """
        record = {
            'market_id': market_id,
            'condition_id': condition_id,
            'open_interest': oi_value,
            'timestamp': datetime.now().isoformat()
        }
        
        self.insert_or_replace('market_open_interest', record)
        
        # Update market with open interest
        self.update_record(
            'markets',
            {'open_interest': oi_value, 'updated_at': datetime.now().isoformat()},
            'id = ?',
            (market_id,)
        )
        
        self.logger.debug(f"Stored open interest for market {market_id}: ${oi_value:,.2f}")
    
    def process_all_markets_detailed(self, use_parallel: bool = True):
        """
        Process all markets to fetch detailed information
        
        Args:
            use_parallel: If True, uses parallel fetching for sub-requests (tags, OI)
        """
        # Get all market IDs from database
        markets = self.fetch_all("SELECT id, condition_id FROM markets ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(markets)} markets for detailed information using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Choose which fetch method to use
            fetch_method = self.fetch_market_by_id_parallel if use_parallel else self.fetch_market_by_id
            
            # Submit all tasks
            future_to_market = {
                executor.submit(self._process_market_detailed, market, fetch_method, len(markets)): market 
                for market in markets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error processing market {market['id']}: {e}")
        
        self.logger.info(f"Market processing complete. Processed: {self._progress_counter}, Errors: {self._error_counter}")
    
    def _process_market_detailed(self, market: Dict, fetch_method, total_markets: int):
        """
        Helper method to process a single market
        Thread-safe wrapper for parallel execution
        """
        try:
            fetch_method(market['id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 50 == 0:
                    self.logger.info(f"Processed {self._progress_counter}/{total_markets} markets")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e
    
    def batch_fetch_markets(self, market_ids: List[str], use_parallel: bool = True) -> List[Dict]:
        """
        Fetch multiple markets in parallel
        
        Args:
            market_ids: List of market IDs to fetch
            use_parallel: If True, uses parallel fetching for sub-requests
            
        Returns:
            List of market dictionaries
        """
        self.logger.info(f"Batch fetching {len(market_ids)} markets using {self.max_workers} threads...")
        
        markets = []
        fetch_method = self.fetch_market_by_id_parallel if use_parallel else self.fetch_market_by_id
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_id = {
                executor.submit(fetch_method, market_id): market_id 
                for market_id in market_ids
            }
            
            for future in as_completed(future_to_id):
                market_id = future_to_id[future]
                try:
                    market = future.result()
                    if market:
                        markets.append(market)
                except Exception as e:
                    self.logger.error(f"Error fetching market {market_id}: {e}")
        
        return markets
    
    def fetch_global_open_interest(self) -> Optional[float]:
        """
        Fetch global open interest across all markets
        """
        try:
            url = f"{self.data_api_url}/oi"
            params = {"market": "GLOBAL"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                global_oi = data[0].get('value', 0)
                
                # Store global OI
                record = {
                    'market_id': 'GLOBAL',
                    'condition_id': 'GLOBAL',
                    'open_interest': global_oi,
                    'timestamp': datetime.now().isoformat()
                }
                self.insert_or_replace('market_open_interest', record)
                
                self.logger.info(f"Global Open Interest: ${global_oi:,.2f}")
                return global_oi
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching global open interest: {e}")
            return None
    
    def daily_scan(self, use_parallel: bool = True):
        """
        Perform daily scan for market updates
        
        Args:
            use_parallel: If True, uses parallel fetching optimizations
        """
        self.logger.info("Starting daily market scan with multithreading...")
        
        # Get all active events
        events = self.fetch_all("SELECT id FROM events WHERE active = 1")
        
        # Fetch markets for active events (parallelized)
        all_markets = self.fetch_all_markets_from_events(events)
        
        # Process detailed information for markets (parallelized)
        self.process_all_markets_detailed(use_parallel=use_parallel)
        
        # Fetch global open interest
        if self.config.FETCH_OPEN_INTEREST:
            self.fetch_global_open_interest()
        
        self.logger.info(f"Daily market scan complete. Total markets: {len(all_markets)}")
        
        return len(all_markets)