"""
Markets Manager for Polymarket Terminal
Handles fetching, processing, and storing market data
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from .database_manager import DatabaseManager
from .config import Config

class MarketsManager(DatabaseManager):
    """Manager for market-related operations"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
    
    def fetch_all_markets_from_events(self, events: List[Dict]) -> List[Dict]:
        """
        Fetch all markets from a list of events
        Used after fetching events to get their markets
        """
        all_markets = []
        total_events = len(events)
        
        self.logger.info(f"Fetching markets for {total_events} events...")
        
        for idx, event in enumerate(events, 1):
            event_id = event.get('id') if isinstance(event, dict) else event
            
            try:
                # Fetch markets for this event
                markets = self._fetch_markets_for_event(event_id)
                
                if markets:
                    all_markets.extend(markets)
                    self._store_markets(markets, event_id)
                
                # Progress logging
                if idx % 10 == 0 or idx == total_events:
                    self.logger.info(f"Progress: {idx}/{total_events} events processed")
                    self.logger.info(f"Total markets fetched: {len(all_markets)}")
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error fetching markets for event {event_id}: {e}")
        
        self.logger.info(f"Total markets fetched: {len(all_markets)}")
        return all_markets
    
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
        """
        market_records = []
        
        for market in markets:
            record = self._prepare_market_record(market, event_id)
            market_records.append(record)
        
        # Bulk insert markets
        if market_records:
            self.bulk_insert_or_replace('markets', market_records)
            self.logger.debug(f"Stored {len(market_records)} markets")
    
    def _store_market_detailed(self, market: Dict):
        """
        Store detailed market information
        """
        # Extract event_id from market data
        event_id = None
        if 'events' in market and market['events']:
            event_id = market['events'][0].get('id')
        
        record = self._prepare_market_record(market, event_id)
        
        self.insert_or_replace('markets', record)
        self.logger.debug(f"Stored detailed market: {market.get('id')}")
    
    def _prepare_market_record(self, market: Dict, event_id: str = None) -> Dict:
        """
        Prepare a market record for database insertion
        """
        return {
            'id': market.get('id'),
            'event_id': event_id or market.get('eventId'),
            'question': market.get('question'),
            'condition_id': market.get('conditionId'),
            'slug': market.get('slug'),
            'resolution_source': market.get('resolutionSource'),
            'description': market.get('description'),
            'start_date': market.get('startDate'),
            'end_date': market.get('endDate'),
            'end_date_iso': market.get('endDateIso'),
            'start_date_iso': market.get('startDateIso'),
            'image': market.get('image'),
            'icon': market.get('icon'),
            'outcomes': json.dumps(market.get('outcomes')) if market.get('outcomes') else None,
            'outcome_prices': json.dumps(market.get('outcomePrices')) if market.get('outcomePrices') else None,
            'clob_token_ids': json.dumps(market.get('clobTokenIds')) if market.get('clobTokenIds') else None,
            'liquidity': market.get('liquidity'),
            'liquidity_num': market.get('liquidityNum'),
            'liquidity_clob': market.get('liquidityClob'),
            'volume': market.get('volume'),
            'volume_num': market.get('volumeNum'),
            'volume_clob': market.get('volumeClob'),
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
    
    def _store_market_tags(self, market_id: str, tags: List[Dict]):
        """
        Store tags and relationships for a market
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
        
        # Bulk insert
        if tag_records:
            self.bulk_insert_or_replace('tags', tag_records)
        if relationships:
            self.bulk_insert_or_replace('market_tags', relationships)
        
        self.logger.debug(f"Stored {len(tags)} tags for market {market_id}")
    
    def _store_open_interest(self, market_id: str, condition_id: str, oi_value: float):
        """
        Store open interest data for a market
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
    
    def process_all_markets_detailed(self):
        """
        Process all markets to fetch detailed information
        """
        # Get all market IDs from database
        markets = self.fetch_all("SELECT id, condition_id FROM markets ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(markets)} markets for detailed information...")
        
        processed = 0
        errors = 0
        
        for market in markets:
            try:
                self.fetch_market_by_id(market['id'])
                processed += 1
                
                if processed % 50 == 0:
                    self.logger.info(f"Processed {processed}/{len(markets)} markets")
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing market {market['id']}: {e}")
                errors += 1
        
        self.logger.info(f"Market processing complete. Processed: {processed}, Errors: {errors}")
    
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
    
    def daily_scan(self):
        """
        Perform daily scan for market updates
        """
        self.logger.info("Starting daily market scan...")
        
        # Get all active events
        events = self.fetch_all("SELECT id FROM events WHERE active = 1")
        
        # Fetch markets for active events
        all_markets = self.fetch_all_markets_from_events(events)
        
        # Process detailed information for markets
        self.process_all_markets_detailed()
        
        # Fetch global open interest
        if self.config.FETCH_OPEN_INTEREST:
            self.fetch_global_open_interest()
        
        self.logger.info(f"Daily market scan complete. Total markets: {len(all_markets)}")
        
        return len(all_markets)