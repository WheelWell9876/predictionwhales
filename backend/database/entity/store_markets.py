from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreMarkets(DatabaseManager):
    """Manager for storing markets with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ...config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0


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