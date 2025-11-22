"""
Store Markets
Handles comprehensive storage functionality for markets data and all related tables
"""

from datetime import datetime
import json
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StoreMarketsManager(DatabaseManager):
    """Manager for storing market data with thread-safe operations"""

    def __init__(self):
        super().__init__()
        from backend.config import Config
        self.config = Config
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()

    def _store_markets(self, markets: List[Dict], event_id: str = None):
        """
        Store multiple markets in the database (thread-safe)
        """
        market_records = []
        market_tags_to_store = []
        market_categories_to_store = []
        image_optimized_to_store = []
        
        for market in markets:
            market_record = self._prepare_market_record(market, event_id)
            market_records.append(market_record)
            
            # Collect tags for this market
            if 'tags' in market and market['tags']:
                for tag in market['tags']:
                    market_tags_to_store.append((market['id'], tag))
            
            # Collect categories for this market
            if 'categories' in market and market['categories']:
                for category in market['categories']:
                    market_categories_to_store.append((market['id'], category))
            
            # Collect image optimization data
            if 'imageOptimized' in market and market['imageOptimized']:
                image_optimized_to_store.append((market['id'], market['imageOptimized'], 'image'))
            if 'iconOptimized' in market and market['iconOptimized']:
                image_optimized_to_store.append((market['id'], market['iconOptimized'], 'icon'))
        
        if market_records:
            with self._db_lock:
                # Store markets
                self.bulk_insert_or_replace('markets', market_records)
                self.logger.debug(f"Stored {len(market_records)} markets for event {event_id}")
                
                # Store tags
                if market_tags_to_store:
                    self._store_market_tags_batch(market_tags_to_store)
                
                # Store categories
                if market_categories_to_store:
                    self._store_market_categories_batch(market_categories_to_store)
                
                # Store image optimization data
                if image_optimized_to_store:
                    self._store_image_optimized_batch(image_optimized_to_store)

    def _store_market_detailed(self, market: Dict):
        """
        Store detailed market information (thread-safe)
        """
        market_record = self._prepare_market_record(market, market.get('eventId'))
        
        with self._db_lock:
            self.insert_or_replace('markets', market_record)
            self.logger.debug(f"Stored detailed market {market.get('id')}")
            
            # Store related data
            if 'tags' in market and market['tags']:
                self._store_market_tags_batch([(market['id'], tag) for tag in market['tags']])
            
            if 'categories' in market and market['categories']:
                self._store_market_categories_batch([(market['id'], cat) for cat in market['categories']])
            
            # Store image optimization data
            if 'imageOptimized' in market and market['imageOptimized']:
                self._store_image_optimized_single(market['id'], market['imageOptimized'], 'image')
            if 'iconOptimized' in market and market['iconOptimized']:
                self._store_image_optimized_single(market['id'], market['iconOptimized'], 'icon')

    def _prepare_market_record(self, market: Dict, event_id: str = None) -> Dict:
        """
        Prepare a comprehensive market record for database insertion
        """
        return {
            'id': market.get('id'),
            'event_id': event_id or market.get('eventId'),
            'question': market.get('question'),
            'condition_id': market.get('conditionId'),
            'slug': market.get('slug'),
            'twitter_card_image': market.get('twitterCardImage'),
            'resolution_source': market.get('resolutionSource'),
            'end_date': market.get('endDate'),
            'start_date': market.get('startDate'),
            'category': market.get('category'),
            'subcategory': market.get('subcategory'),
            'amm_type': market.get('ammType'),
            'liquidity': market.get('liquidity'),
            'liquidity_num': self._safe_float(market.get('liquidityNum', market.get('liquidity'))),
            'liquidity_amm': self._safe_float(market.get('liquidityAmm')),
            'liquidity_clob': self._safe_float(market.get('liquidityClob')),
            'sponsor_name': market.get('sponsorName'),
            'sponsor_image': market.get('sponsorImage'),
            'x_axis_value': market.get('xAxisValue'),
            'y_axis_value': market.get('yAxisValue'),
            'denomination_token': market.get('denominationToken'),
            'fee': market.get('fee'),
            'image': market.get('image'),
            'icon': market.get('icon'),
            'lower_bound': market.get('lowerBound'),
            'upper_bound': market.get('upperBound'),
            'lower_bound_date': market.get('lowerBoundDate'),
            'upper_bound_date': market.get('upperBoundDate'),
            'description': market.get('description'),
            'outcomes': json.dumps(market.get('outcomes')) if market.get('outcomes') else None,
            'outcome_prices': json.dumps(market.get('outcomePrices')) if market.get('outcomePrices') else None,
            'short_outcomes': json.dumps(market.get('shortOutcomes')) if market.get('shortOutcomes') else None,
            'volume': market.get('volume'),
            'volume_num': self._safe_float(market.get('volumeNum', market.get('volume'))),
            'volume_amm': self._safe_float(market.get('volumeAmm')),
            'volume_clob': self._safe_float(market.get('volumeClob')),
            'volume_24hr': self._safe_float(market.get('volume24hr')),
            'volume_24hr_amm': self._safe_float(market.get('volume24hrAmm')),
            'volume_24hr_clob': self._safe_float(market.get('volume24hrClob')),
            'volume_1wk': self._safe_float(market.get('volume1wk')),
            'volume_1wk_amm': self._safe_float(market.get('volume1wkAmm')),
            'volume_1wk_clob': self._safe_float(market.get('volume1wkClob')),
            'volume_1mo': self._safe_float(market.get('volume1mo')),
            'volume_1mo_amm': self._safe_float(market.get('volume1moAmm')),
            'volume_1mo_clob': self._safe_float(market.get('volume1moClob')),
            'volume_1yr': self._safe_float(market.get('volume1yr')),
            'volume_1yr_amm': self._safe_float(market.get('volume1yrAmm')),
            'volume_1yr_clob': self._safe_float(market.get('volume1yrClob')),
            'active': int(market.get('active', True)),
            'closed': int(market.get('closed', False)),
            'archived': int(market.get('archived', False)),
            'new': int(market.get('new', False)),
            'featured': int(market.get('featured', False)),
            'restricted': int(market.get('restricted', False)),
            'market_type': market.get('marketType'),
            'format_type': market.get('formatType'),
            'market_maker_address': market.get('marketMakerAddress'),
            'created_by': market.get('createdBy'),
            'updated_by': market.get('updatedBy'),
            'created_at': market.get('createdAt'),
            'updated_at': market.get('updatedAt'),
            'closed_time': market.get('closedTime'),
            'wide_format': int(market.get('wideFormat', False)),
            'mailchimp_tag': market.get('mailchimpTag'),
            'resolved_by': market.get('resolvedBy'),
            'market_group': market.get('marketGroup'),
            'group_item_title': market.get('groupItemTitle'),
            'group_item_threshold': market.get('groupItemThreshold'),
            'group_item_range': market.get('groupItemRange'),
            'question_id': market.get('questionID'),
            'uma_end_date': market.get('umaEndDate'),
            'uma_end_date_iso': market.get('umaEndDateIso'),
            'enable_order_book': int(market.get('enableOrderBook', False)),
            'order_price_min_tick_size': self._safe_float(market.get('orderPriceMinTickSize')),
            'order_min_size': self._safe_float(market.get('orderMinSize')),
            'uma_resolution_status': market.get('umaResolutionStatus'),
            'uma_resolution_statuses': json.dumps(market.get('umaResolutionStatuses')) if market.get('umaResolutionStatuses') else None,
            'curation_order': market.get('curationOrder'),
            'end_date_iso': market.get('endDateIso'),
            'start_date_iso': market.get('startDateIso'),
            'has_reviewed_dates': int(market.get('hasReviewedDates', False)),
            'ready_for_cron': int(market.get('readyForCron', False)),
            'comments_enabled': int(market.get('commentsEnabled', False)),
            'game_start_time': market.get('gameStartTime'),
            'seconds_delay': market.get('secondsDelay'),
            'clob_token_ids': json.dumps(market.get('clobTokenIds')) if market.get('clobTokenIds') else None,
            'disqus_thread': market.get('disqusThread'),
            'team_a_id': market.get('teamAID'),
            'team_b_id': market.get('teamBID'),
            'uma_bond': market.get('umaBond'),
            'uma_reward': market.get('umaReward'),
            'fpmmLive': int(market.get('fpmmLive', False)),
            'maker_base_fee': self._safe_float(market.get('makerBaseFee')),
            'taker_base_fee': self._safe_float(market.get('takerBaseFee')),
            'custom_liveness': market.get('customLiveness'),
            'accepting_orders': int(market.get('acceptingOrders', False)),
            'accepting_orders_timestamp': market.get('acceptingOrdersTimestamp'),
            'notifications_enabled': int(market.get('notificationsEnabled', False)),
            'score': self._safe_float(market.get('score')),
            'creator': market.get('creator'),
            'ready': int(market.get('ready', False)),
            'funded': int(market.get('funded', False)),
            'past_slugs': json.dumps(market.get('pastSlugs')) if market.get('pastSlugs') else None,
            'ready_timestamp': market.get('readyTimestamp'),
            'funded_timestamp': market.get('fundedTimestamp'),
            'competitive': self._safe_float(market.get('competitive')),
            'rewards_min_size': self._safe_float(market.get('rewardsMinSize')),
            'rewards_max_spread': self._safe_float(market.get('rewardsMaxSpread')),
            'spread': self._safe_float(market.get('spread')),
            'automatically_resolved': int(market.get('automaticallyResolved', False)),
            'one_day_price_change': self._safe_float(market.get('oneDayPriceChange')),
            'one_hour_price_change': self._safe_float(market.get('oneHourPriceChange')),
            'one_week_price_change': self._safe_float(market.get('oneWeekPriceChange')),
            'one_month_price_change': self._safe_float(market.get('oneMonthPriceChange')),
            'one_year_price_change': self._safe_float(market.get('oneYearPriceChange')),
            'last_trade_price': self._safe_float(market.get('lastTradePrice')),
            'best_bid': self._safe_float(market.get('bestBid')),
            'best_ask': self._safe_float(market.get('bestAsk')),
            'automatically_active': int(market.get('automaticallyActive', False)),
            'clear_book_on_start': int(market.get('clearBookOnStart', False)),
            'chart_color': market.get('chartColor'),
            'series_color': market.get('seriesColor'),
            'show_gmp_series': int(market.get('showGmpSeries', False)),
            'show_gmp_outcome': int(market.get('showGmpOutcome', False)),
            'manual_activation': int(market.get('manualActivation', False)),
            'neg_risk': int(market.get('negRisk', False)),
            'neg_risk_other': int(market.get('negRiskOther', False)),
            'game_id': market.get('gameId'),
            'sports_market_type': market.get('sportsMarketType'),
            'line': self._safe_float(market.get('line')),
            'pending_deployment': int(market.get('pendingDeployment', False)),
            'deploying': int(market.get('deploying', False)),
            'deploying_timestamp': market.get('deployingTimestamp'),
            'scheduled_deployment_timestamp': market.get('scheduledDeploymentTimestamp'),
            'rfq_enabled': int(market.get('rfqEnabled', False)),
            'event_start_time': market.get('eventStartTime'),
            'fetched_at': datetime.now().isoformat()
        }

    def _safe_float(self, value):
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _store_market_tags_batch(self, market_tags: List[tuple]):
        """Store market tags in batch"""
        tag_records = []
        market_tag_records = []
        
        for market_id, tag in market_tags:
            if isinstance(tag, dict):
                tag_id = tag.get('id')
                tag_slug = tag.get('slug')
                tag_label = tag.get('label')
                
                # Store the tag itself
                if tag_id:
                    tag_records.append({
                        'id': tag_id,
                        'slug': tag_slug,
                        'label': tag_label,
                        'force_show': int(tag.get('forceShow', False)),
                        'force_hide': int(tag.get('forceHide', False)),
                        'is_carousel': int(tag.get('isCarousel', False)),
                        'published_at': tag.get('publishedAt'),
                        'created_by': tag.get('createdBy'),
                        'updated_by': tag.get('updatedBy'),
                        'created_at': tag.get('createdAt'),
                        'updated_at': tag.get('updatedAt'),
                        'fetched_at': datetime.now().isoformat()
                    })
            else:
                # Tag is just a string ID
                tag_id = tag
                tag_slug = tag
            
            # Store the market-tag relationship
            if tag_id:
                market_tag_records.append({
                    'market_id': market_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                })
        
        # Insert tags first
        if tag_records:
            self.bulk_insert_or_ignore('tags', tag_records)
        
        # Then insert relationships
        if market_tag_records:
            self.bulk_insert_or_ignore('market_tags', market_tag_records)

    def _store_market_categories_batch(self, market_categories: List[tuple]):
        """Store market categories in batch"""
        category_records = []
        market_category_records = []
        
        for market_id, category in market_categories:
            if isinstance(category, dict):
                cat_id = category.get('id')
                
                # Store the category itself
                if cat_id:
                    category_records.append({
                        'id': cat_id,
                        'label': category.get('label'),
                        'parent_category': category.get('parentCategory'),
                        'slug': category.get('slug'),
                        'published_at': category.get('publishedAt'),
                        'created_by': category.get('createdBy'),
                        'updated_by': category.get('updatedBy'),
                        'created_at': category.get('createdAt'),
                        'updated_at': category.get('updatedAt')
                    })
                    
                    # Store the market-category relationship
                    market_category_records.append({
                        'market_id': market_id,
                        'category_id': cat_id
                    })
        
        # Insert categories first
        if category_records:
            self.bulk_insert_or_ignore('categories', category_records)
        
        # Then insert relationships
        if market_category_records:
            self.bulk_insert_or_ignore('market_categories', market_category_records)

    def _store_image_optimized_batch(self, image_data: List[tuple]):
        """Store image optimization data in batch"""
        image_records = []
        
        for market_id, img_data, field_type in image_data:
            if img_data:
                image_records.append({
                    'id': img_data.get('id'),
                    'image_url_source': img_data.get('imageUrlSource'),
                    'image_url_optimized': img_data.get('imageUrlOptimized'),
                    'image_size_kb_source': self._safe_float(img_data.get('imageSizeKbSource')),
                    'image_size_kb_optimized': self._safe_float(img_data.get('imageSizeKbOptimized')),
                    'image_optimized_complete': int(img_data.get('imageOptimizedComplete', False)),
                    'image_optimized_last_updated': img_data.get('imageOptimizedLastUpdated'),
                    'rel_id': img_data.get('relID'),
                    'field': img_data.get('field', field_type),
                    'relname': img_data.get('relname'),
                    'entity_type': 'market',
                    'entity_id': market_id
                })
        
        if image_records:
            self.bulk_insert_or_ignore('image_optimized', image_records)

    def _store_image_optimized_single(self, market_id: str, img_data: Dict, field_type: str):
        """Store single image optimization record"""
        if img_data:
            record = {
                'id': img_data.get('id'),
                'image_url_source': img_data.get('imageUrlSource'),
                'image_url_optimized': img_data.get('imageUrlOptimized'),
                'image_size_kb_source': self._safe_float(img_data.get('imageSizeKbSource')),
                'image_size_kb_optimized': self._safe_float(img_data.get('imageSizeKbOptimized')),
                'image_optimized_complete': int(img_data.get('imageOptimizedComplete', False)),
                'image_optimized_last_updated': img_data.get('imageOptimizedLastUpdated'),
                'rel_id': img_data.get('relID'),
                'field': img_data.get('field', field_type),
                'relname': img_data.get('relname'),
                'entity_type': 'market',
                'entity_id': market_id
            }
            self.insert_or_ignore('image_optimized', record)

    def store_market_open_interest(self, market_id: str, condition_id: str, open_interest: float):
        """Store market open interest data"""
        record = {
            'market_id': market_id,
            'condition_id': condition_id,
            'timestamp': datetime.now().isoformat(),
            'open_interest': self._safe_float(open_interest)
        }
        
        with self._db_lock:
            self.insert_or_replace('market_open_interest', record)
            self.logger.debug(f"Stored open interest for market {market_id}")

    def store_market_holders(self, market_id: str, holders: List[Dict]):
        """Store market holders data"""
        holder_records = []
        
        for holder in holders:
            holder_records.append({
                'market_id': market_id,
                'proxy_wallet': holder.get('proxyWallet'),
                'shares': self._safe_float(holder.get('shares', 0)),
                'avg_price': self._safe_float(holder.get('avgPrice', 0))
            })
        
        if holder_records:
            with self._db_lock:
                self.bulk_insert_or_replace('market_holders', holder_records)
                self.logger.debug(f"Stored {len(holder_records)} holders for market {market_id}")