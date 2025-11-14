"""
Transactions Manager for Polymarket Terminal - ENHANCED WHALE FOCUSED
Handles fetching whale transactions and comprehensive user trading data
With improved thread safety and batch processing to avoid database locks
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from .database_manager import DatabaseManager

class TransactionsManager(DatabaseManager):
    """Enhanced manager for whale transaction and user trading data operations"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
        self.config = Config
        self.clob_api_url = Config.CLOB_API_URL if Config.CLOB_API_URL else "https://clob.polymarket.com"
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        
        # Reduce max workers to avoid overwhelming the database
        self.max_workers = max_workers or min(5, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 5))
        
        # Thread-safe batch collections
        self._batch_lock = Lock()
        self._batch_data = defaultdict(list)
        
        # Progress tracking
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Whale thresholds
        self.MIN_TRANSACTION_SIZE = Config.MIN_TRANSACTION_SIZE if hasattr(Config, 'MIN_TRANSACTION_SIZE') else 500
        self.MIN_WHALE_TRADE = Config.MIN_WHALE_TRADE if hasattr(Config, 'MIN_WHALE_TRADE') else 10000
        self.MIN_POSITION_VALUE = 500  # Minimum closed position value to fetch

    def fetch_comprehensive_whale_data(self, limit_users: int = None) -> Dict[str, int]:
        """
        Main entry point for fetching comprehensive whale transaction and trading data
        This includes: transactions, positions, activity, portfolio values, and closed positions
        """
        self.logger.info("=" * 60)
        self.logger.info("🐋 STARTING COMPREHENSIVE WHALE DATA FETCH")
        self.logger.info("=" * 60)
        
        results = {
            'whale_users': 0,
            'transactions': 0,
            'current_positions': 0,
            'closed_positions': 0,
            'user_activities': 0,
            'portfolio_values': 0,
            'trades': 0
        }
        
        # Step 1: Get whale users from various sources
        whale_users = self._get_whale_users(limit_users)
        results['whale_users'] = len(whale_users)
        
        if not whale_users:
            self.logger.warning("No whale users found to process")
            return results
        
        self.logger.info(f"Processing {len(whale_users)} whale users...")
        
        # Step 2: Fetch current positions for whales
        self.logger.info("\n📊 Phase 1: Fetching Current Positions...")
        positions_result = self.fetch_user_positions_batch(whale_users)
        results['current_positions'] = positions_result['total_positions']
        
        # Step 3: Fetch user trading activity (with batching)
        self.logger.info("\n📈 Phase 2: Fetching User Activity...")
        activity_result = self.fetch_user_activity_batch(whale_users)
        results['user_activities'] = activity_result['total_activities']
        
        # Step 4: Fetch portfolio values
        self.logger.info("\n💰 Phase 3: Fetching Portfolio Values...")
        values_result = self.fetch_user_values_batch(whale_users)
        results['portfolio_values'] = values_result['values_fetched']
        
        # Step 5: Fetch closed positions
        self.logger.info("\n💸 Phase 4: Fetching Closed Positions...")
        closed_result = self.fetch_closed_positions_batch(whale_users)
        results['closed_positions'] = closed_result['total_positions']
        
        # Step 6: Fetch trades for whales and top markets
        self.logger.info("\n📈 Phase 5: Fetching Whale Trades...")
        trades_result = self.fetch_user_trades_batch(whale_users[:200])  # Limit to top 200 users
        results['trades'] = trades_result['total_trades']
        
        # Step 7: Fetch whale transactions from recent activity
        self.logger.info("\n💸 Phase 6: Fetching Recent Whale Transactions...")
        tx_result = self.fetch_recent_whale_transactions()
        results['transactions'] = tx_result
        
        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("✅ COMPREHENSIVE WHALE DATA FETCH COMPLETE!")
        self.logger.info("=" * 60)
        for key, value in results.items():
            self.logger.info(f"   {key:<20} {value:>10,}")
        
        return results

    def _get_whale_users(self, limit: int = None) -> List[str]:
        """Get unique whale users from various sources"""
        query = """
            SELECT DISTINCT proxy_wallet
            FROM (
                -- Users marked as whales
                SELECT proxy_wallet FROM users WHERE is_whale = 1
                UNION
                -- Users with large transactions
                SELECT DISTINCT proxy_wallet FROM transactions WHERE usdc_size >= ?
                UNION
                -- Users with large positions in market_holders
                SELECT DISTINCT proxy_wallet FROM market_holders WHERE amount >= ?
                UNION
                -- Users with significant trading activity
                SELECT DISTINCT proxy_wallet FROM user_trades WHERE size >= ?
            )
            ORDER BY proxy_wallet
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        users = self.fetch_all(query, (
            self.MIN_TRANSACTION_SIZE,
            self.MIN_TRANSACTION_SIZE / 2,  # Lower threshold for holders
            self.MIN_TRANSACTION_SIZE / 2
        ))
        
        return [u['proxy_wallet'] for u in users]

    def fetch_user_positions_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch current positions for a batch of users using multithreading"""
        self.logger.info(f"Fetching current positions for {len(users)} users...")
        
        total_positions = 0
        users_with_positions = 0
        whale_positions = []
        
        # Collect all positions in memory first
        all_positions = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_user_positions_api, user): user 
                for user in users
            }
            
            for future in as_completed(futures):
                user = futures[future]
                try:
                    result = future.result()
                    if result['positions']:
                        all_positions.extend(result['positions'])
                        users_with_positions += 1
                        total_positions += len(result['positions'])
                        whale_positions.extend(result['whale_positions'])
                        
                except Exception as e:
                    self.logger.error(f"Error fetching positions for {user}: {e}")
        
        # Now bulk insert all positions at once
        if all_positions:
            self._bulk_insert_positions(all_positions)
        
        # Sort and display top whale positions
        whale_positions.sort(key=lambda x: x['value'], reverse=True)
        
        if whale_positions[:5]:
            self.logger.info("🐋 Top 5 Whale Positions:")
            for pos in whale_positions[:5]:
                self.logger.info(f"   {pos['wallet'][:10]}... - {pos['title'][:40]}")
                self.logger.info(f"     Value: ${pos['value']:,.2f} | P&L: {pos['pnl']:.2%}")
        
        return {
            'users_with_positions': users_with_positions,
            'total_positions': total_positions,
            'whale_positions': len([p for p in whale_positions if p['value'] > 10000])
        }

    def _fetch_user_positions_api(self, proxy_wallet: str) -> Dict:
        """Fetch current positions for a single user from API"""
        try:
            url = f"{self.data_api_url}/positions"
            params = {
                "user": proxy_wallet,
                "sizeThreshold": "1",
                "limit": "100",
                "sortBy": "TOKENS",
                "sortDirection": "DESC"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'positions': [], 'whale_positions': []}
            
            positions = response.json()
            
            if not positions:
                return {'positions': [], 'whale_positions': []}
            
            # Prepare positions for batch insert
            processed_positions = []
            whale_positions = []
            user_total_value = 0
            
            for position in positions:
                # Add proxy_wallet to position data
                position['proxyWallet'] = proxy_wallet
                processed_positions.append(position)
                
                user_total_value += position.get('currentValue', 0)
                
                # Track whale positions (>$10k value)
                if position.get('currentValue', 0) > 10000:
                    whale_positions.append({
                        'wallet': proxy_wallet,
                        'title': position.get('title'),
                        'value': position.get('currentValue'),
                        'pnl': position.get('percentPnl', 0),
                        'outcome': position.get('outcome')
                    })
            
            # Update user total value (done separately)
            if user_total_value > 0:
                self.execute_query("""
                    UPDATE users SET total_value = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE proxy_wallet = ?
                """, (user_total_value, proxy_wallet), commit=True)
            
            return {
                'positions': processed_positions,
                'whale_positions': whale_positions
            }
            
        except Exception as e:
            self.logger.debug(f"Error fetching positions for {proxy_wallet}: {e}")
            return {'positions': [], 'whale_positions': []}

    def _bulk_insert_positions(self, positions: List[Dict]):
        """Bulk insert positions into database"""
        if not positions:
            return
        
        # Prepare data for bulk insert
        position_data = []
        
        for position in positions:
            position_data.append({
                'proxy_wallet': position.get('proxyWallet'),
                'asset': position.get('asset'),
                'condition_id': position.get('conditionId'),
                'size': position.get('size'),
                'avg_price': position.get('avgPrice'),
                'initial_value': position.get('initialValue'),
                'current_value': position.get('currentValue'),
                'cash_pnl': position.get('cashPnl'),
                'percent_pnl': position.get('percentPnl'),
                'total_bought': position.get('totalBought'),
                'realized_pnl': position.get('realizedPnl'),
                'percent_realized_pnl': position.get('percentRealizedPnl'),
                'cur_price': position.get('curPrice'),
                'redeemable': position.get('redeemable'),
                'mergeable': position.get('mergeable'),
                'negative_risk': position.get('negativeRisk'),
                'title': position.get('title'),
                'slug': position.get('slug'),
                'icon': position.get('icon'),
                'event_id': position.get('eventId'),
                'event_slug': position.get('eventSlug'),
                'outcome': position.get('outcome'),
                'outcome_index': position.get('outcomeIndex'),
                'opposite_outcome': position.get('oppositeOutcome'),
                'opposite_asset': position.get('oppositeAsset'),
                'end_date': position.get('endDate')
            })
        
        # Bulk insert
        self.bulk_insert_or_replace('user_positions_current', position_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(position_data)} positions")

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

    def _fetch_user_activity_api(self, proxy_wallet: str) -> List[Dict]:
        """Fetch activity for a single user from API"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {
                "user": proxy_wallet,
                "limit": 500,  # Reduced from 1000 to be more conservative
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

    def _bulk_insert_activities(self, activities: List[Dict]):
        """Bulk insert activities into database"""
        if not activities:
            return
        
        # Prepare data for bulk insert
        activity_data = []
        
        for activity in activities:
            activity_data.append({
                'proxy_wallet': activity.get('proxyWallet'),
                'timestamp': activity.get('timestamp'),
                'condition_id': activity.get('conditionId'),
                'transaction_hash': activity.get('transactionHash'),
                'type': activity.get('type'),
                'side': activity.get('side'),
                'size': activity.get('size'),
                'usdc_size': activity.get('usdcSize'),
                'price': activity.get('price'),
                'asset': activity.get('asset'),
                'outcome_index': activity.get('outcomeIndex'),
                'title': activity.get('title'),
                'slug': activity.get('slug'),
                'event_slug': activity.get('eventSlug'),
                'outcome': activity.get('outcome'),
                'username': activity.get('name'),
                'pseudonym': activity.get('pseudonym'),
                'bio': activity.get('bio'),
                'profile_image': activity.get('profileImage')
            })
        
        # Bulk insert with INSERT OR IGNORE to avoid duplicates
        self.bulk_insert_or_ignore('user_activity', activity_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(activity_data)} activities")

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

    def _bulk_insert_values(self, values: List[Dict]):
        """Bulk insert portfolio values"""
        if not values:
            return
        
        value_data = []
        user_updates = []
        
        for val in values:
            if val['fetched']:
                value_data.append({
                    'proxy_wallet': val['wallet'],
                    'market_condition_id': None,
                    'value': val['value']
                })
                
                user_updates.append({
                    'proxy_wallet': val['wallet'],
                    'total_value': val['value'],
                    'last_updated': datetime.now()
                })
        
        # Bulk insert values
        if value_data:
            self.bulk_insert_or_replace('user_values', value_data, batch_size=100)
            
            # Update users table
            for update in user_updates:
                self.execute_query("""
                    UPDATE users SET total_value = ?, last_updated = ?
                    WHERE proxy_wallet = ?
                """, (update['total_value'], update['last_updated'], update['proxy_wallet']), commit=False)
            
            # Commit all updates
            conn = self.get_connection()
            conn.commit()
            
        self.logger.info(f"Bulk inserted {len(value_data)} portfolio values")

    def fetch_closed_positions_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch closed positions for a batch of users"""
        self.logger.info(f"Fetching closed positions for {len(users)} users...")
        
        all_positions = []
        big_winners = []
        big_losers = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
            chunk_size = 50
            for i in range(0, len(users), chunk_size):
                chunk = users[i:i+chunk_size]
                
                futures = {
                    executor.submit(self._fetch_closed_positions_api, user): user 
                    for user in chunk
                }
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        all_positions.extend(result['positions'])
                        big_winners.extend(result['winners'])
                        big_losers.extend(result['losers'])
                    except Exception as e:
                        self.logger.debug(f"Error in closed positions batch: {e}")
                
                time.sleep(0.5)  # Small delay between chunks
        
        # Bulk insert positions
        if all_positions:
            self._bulk_insert_closed_positions(all_positions)
        
        # Sort and display top winners/losers
        big_winners.sort(key=lambda x: x['pnl'], reverse=True)
        big_losers.sort(key=lambda x: x['pnl'])
        
        if big_winners[:3]:
            self.logger.info("💰 Top 3 Winning Trades:")
            for win in big_winners[:3]:
                self.logger.info(f"   {win['wallet'][:10]}... - P&L: ${win['pnl']:,.2f}")
        
        if big_losers[:3]:
            self.logger.info("💸 Top 3 Losing Trades:")
            for loss in big_losers[:3]:
                self.logger.info(f"   {loss['wallet'][:10]}... - P&L: ${loss['pnl']:,.2f}")
        
        return {
            'total_positions': len(all_positions),
            'big_winners': len(big_winners),
            'big_losers': len(big_losers)
        }

    def _fetch_closed_positions_api(self, proxy_wallet: str) -> Dict:
        """Fetch closed positions for a single user from API"""
        try:
            url = f"{self.data_api_url}/closed-positions"
            params = {
                "user": proxy_wallet,
                "limit": "50",
                "sortBy": "REALIZEDPNL",
                "sortDirection": "DESC"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'positions': [], 'winners': [], 'losers': []}
            
            closed_positions = response.json()
            
            if not closed_positions:
                return {'positions': [], 'winners': [], 'losers': []}
            
            processed_positions = []
            winners = []
            losers = []
            
            for position in closed_positions:
                realized_pnl = position.get('realizedPnl', 0)
                
                # Only store positions above minimum threshold
                if abs(realized_pnl) < self.MIN_POSITION_VALUE:
                    continue
                
                position['proxyWallet'] = proxy_wallet
                processed_positions.append(position)
                
                # Track big winners and losers
                if realized_pnl > 5000:
                    winners.append({
                        'wallet': proxy_wallet,
                        'title': position.get('title'),
                        'pnl': realized_pnl,
                        'outcome': position.get('outcome')
                    })
                elif realized_pnl < -5000:
                    losers.append({
                        'wallet': proxy_wallet,
                        'title': position.get('title'),
                        'pnl': realized_pnl,
                        'outcome': position.get('outcome')
                    })
            
            return {'positions': processed_positions, 'winners': winners, 'losers': losers}
            
        except Exception as e:
            self.logger.debug(f"Error fetching closed positions for {proxy_wallet}: {e}")
            return {'positions': [], 'winners': [], 'losers': []}

    def _bulk_insert_closed_positions(self, positions: List[Dict]):
        """Bulk insert closed positions"""
        if not positions:
            return
        
        position_data = []
        
        for position in positions:
            position_data.append({
                'proxy_wallet': position.get('proxyWallet'),
                'asset': position.get('asset'),
                'condition_id': position.get('conditionId'),
                'avg_price': position.get('avgPrice'),
                'total_bought': position.get('totalBought'),
                'realized_pnl': position.get('realizedPnl'),
                'cur_price': position.get('curPrice'),
                'title': position.get('title'),
                'slug': position.get('slug'),
                'icon': position.get('icon'),
                'event_slug': position.get('eventSlug'),
                'outcome': position.get('outcome'),
                'outcome_index': position.get('outcomeIndex'),
                'opposite_outcome': position.get('oppositeOutcome'),
                'opposite_asset': position.get('oppositeAsset'),
                'end_date': position.get('endDate')
            })
        
        self.bulk_insert_or_ignore('user_positions_closed', position_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(position_data)} closed positions")

    def fetch_user_trades_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch trades for a batch of users"""
        self.logger.info(f"Fetching trades for {len(users)} users...")
        
        all_trades = []
        whale_trades = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
            chunk_size = 50
            for i in range(0, len(users), chunk_size):
                chunk = users[i:i+chunk_size]
                
                futures = {
                    executor.submit(self._fetch_user_trades_api, user): user 
                    for user in chunk
                }
                
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        all_trades.extend(result['trades'])
                        whale_trades.extend(result['whale_trades'])
                    except Exception as e:
                        self.logger.debug(f"Error in trades batch: {e}")
                
                time.sleep(0.5)
        
        # Bulk insert trades
        if all_trades:
            self._bulk_insert_trades(all_trades)
        
        # Also fetch trades for top markets
        market_trades = self._fetch_market_trades()
        
        return {
            'total_trades': len(all_trades) + market_trades,
            'whale_trades': len(whale_trades)
        }

    def _fetch_user_trades_api(self, proxy_wallet: str) -> Dict:
        """Fetch trades for a single user from API"""
        try:
            url = f"{self.data_api_url}/trades"
            params = {
                "user": proxy_wallet,
                "limit": "100",
                "takerOnly": "false"
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code != 200:
                return {'trades': [], 'whale_trades': []}
            
            trades = response.json()
            
            if not trades:
                return {'trades': [], 'whale_trades': []}
            
            processed_trades = []
            whale_trades = []
            
            for trade in trades:
                trade_value = trade.get('size', 0) * trade.get('price', 0)
                
                trade['proxyWallet'] = proxy_wallet
                processed_trades.append(trade)
                
                # Track whale trades
                if trade_value > 10000:
                    whale_trades.append({
                        'wallet': proxy_wallet,
                        'value': trade_value,
                        'side': trade.get('side')
                    })
            
            return {'trades': processed_trades, 'whale_trades': whale_trades}
            
        except Exception as e:
            self.logger.debug(f"Error fetching trades for {proxy_wallet}: {e}")
            return {'trades': [], 'whale_trades': []}

    def _bulk_insert_trades(self, trades: List[Dict]):
        """Bulk insert trades"""
        if not trades:
            return
        
        trade_data = []
        
        for trade in trades:
            trade_data.append({
                'proxy_wallet': trade.get('proxyWallet'),
                'side': trade.get('side'),
                'asset': trade.get('asset'),
                'condition_id': trade.get('conditionId'),
                'size': trade.get('size'),
                'price': trade.get('price'),
                'timestamp': trade.get('timestamp'),
                'transaction_hash': trade.get('transactionHash'),
                'title': trade.get('title'),
                'slug': trade.get('slug'),
                'icon': trade.get('icon'),
                'event_slug': trade.get('eventSlug'),
                'outcome': trade.get('outcome'),
                'outcome_index': trade.get('outcomeIndex'),
                'username': trade.get('name'),
                'pseudonym': trade.get('pseudonym'),
                'bio': trade.get('bio'),
                'profile_image': trade.get('profileImage'),
                'profile_image_optimized': trade.get('profileImageOptimized')
            })
        
        self.bulk_insert_or_ignore('user_trades', trade_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(trade_data)} trades")

    def _fetch_market_trades(self) -> int:
        """Fetch trades for top markets"""
        self.logger.info("Fetching trades for top markets...")
        
        # Get top markets by volume
        markets = self.fetch_all("""
            SELECT DISTINCT condition_id, question
            FROM markets
            WHERE active = 1 AND volume > 100000
            ORDER BY volume DESC
            LIMIT 10
        """)
        
        all_trades = []
        
        for market in markets:
            try:
                url = f"{self.data_api_url}/trades"
                params = {
                    "market": market['condition_id'],
                    "limit": "50"
                }
                
                response = requests.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    trades = response.json()
                    
                    for trade in trades:
                        # Only store significant trades
                        trade_value = trade.get('size', 0) * trade.get('price', 0)
                        if trade_value >= self.MIN_TRANSACTION_SIZE:
                            all_trades.append(trade)
                            
            except Exception as e:
                self.logger.debug(f"Error fetching market trades: {e}")
        
        # Bulk insert market trades
        if all_trades:
            self._bulk_insert_trades(all_trades)
        
        return len(all_trades)

    def fetch_recent_whale_transactions(self) -> int:
        """Fetch recent whale transactions from CLOB API"""
        self.logger.info("Fetching recent whale transactions...")
        
        try:
            # Get recent whale users with activity
            recent_whales = self.fetch_all("""
                SELECT DISTINCT proxy_wallet
                FROM user_activity
                WHERE usdc_size >= ?
                AND timestamp > datetime('now', '-7 days')
                ORDER BY usdc_size DESC
                LIMIT 50
            """, (self.MIN_WHALE_TRADE,))
            
            all_transactions = []
            
            for whale in recent_whales:
                try:
                    # Fetch transactions for this whale
                    url = f"{self.clob_api_url}/trades"
                    params = {
                        "user": whale['proxy_wallet'],
                        "limit": 25,
                        "minSize": self.MIN_TRANSACTION_SIZE
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        transactions = response.json()
                        
                        for tx in transactions:
                            usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
                            
                            if usdc_size >= self.MIN_TRANSACTION_SIZE:
                                tx['is_whale'] = 1 if usdc_size >= self.MIN_WHALE_TRADE else 0
                                all_transactions.append(tx)
                                
                except Exception as e:
                    self.logger.debug(f"Error fetching transactions for whale: {e}")
            
            # Bulk insert transactions
            if all_transactions:
                self._bulk_insert_transactions(all_transactions)
            
            self.logger.info(f"Fetched {len(all_transactions)} whale transactions")
            return len(all_transactions)
            
        except Exception as e:
            self.logger.error(f"Error fetching whale transactions: {e}")
            return 0

    def _bulk_insert_transactions(self, transactions: List[Dict]):
        """Bulk insert transactions"""
        if not transactions:
            return
        
        tx_data = []
        
        for tx in transactions:
            usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
            
            tx_data.append({
                'transaction_hash': tx.get('transactionHash'),
                'proxy_wallet': tx.get('proxyWallet'),
                'timestamp': tx.get('timestamp'),
                'market_id': tx.get('marketId'),
                'condition_id': tx.get('conditionId'),
                'side': tx.get('side'),
                'size': tx.get('size'),
                'price': tx.get('price'),
                'usdc_size': usdc_size,
                'type': tx.get('type', 'trade'),
                'username': tx.get('name'),
                'pseudonym': tx.get('pseudonym'),
                'is_whale': tx.get('is_whale', 0)
            })
        
        self.bulk_insert_or_replace('transactions', tx_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(tx_data)} transactions")

    def fetch_market_transactions(self, market_id: str, condition_id: str, limit: int = 100) -> List[Dict]:
        """
        Legacy method - now calls comprehensive fetch for specific market
        """
        try:
            url = f"{self.data_api_url}/trades"
            params = {
                "market": condition_id,
                "limit": limit,
                "minSize": self.MIN_TRANSACTION_SIZE
            }
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                transactions = response.json()
                
                whale_transactions = []
                for tx in transactions:
                    usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
                    if usdc_size >= self.MIN_TRANSACTION_SIZE:
                        tx['is_whale'] = 1 if usdc_size >= self.MIN_WHALE_TRADE else 0
                        whale_transactions.append(tx)
                
                if whale_transactions:
                    self._bulk_insert_transactions(whale_transactions)
                
                return whale_transactions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching market transactions: {e}")
            return []