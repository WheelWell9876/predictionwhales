"""
Users Manager for Polymarket Terminal
Handles fetching, processing, and storing user data including positions, trades, and activity
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from .database_manager import DatabaseManager

class UsersManager(DatabaseManager):
    """Manager for user-related operations"""

    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        self.base_url = Config.GAMMA_API_URL if Config.GAMMA_API_URL else "https://gamma-api.polymarket.com"

    def fetch_user_current_positions(self, proxy_wallet: str) -> List[Dict]:
        """
        Fetch current positions for a specific user
        """
        try:
            url = f"{self.data_api_url}/positions"
            params = {
                "user": proxy_wallet,
                "sizeThreshold": "1",
                "limit": "100",
                "sortBy": "TOKENS",
                "sortDirection": "DESC"
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                positions = response.json()

                if positions:
                    self._store_user_positions(proxy_wallet, positions)
                    return positions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching positions for {proxy_wallet}: {e}")
            return []

    def fetch_user_trades(self, proxy_wallet: str, limit: int = 100) -> List[Dict]:
        """
        Fetch trades for a specific user
        """
        try:
            url = f"{self.data_api_url}/trades"
            params = {
                "user": proxy_wallet,
                "limit": limit,
                "takerOnly": "false"
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                trades = response.json()

                if trades:
                    self._store_user_trades(proxy_wallet, trades)
                    return trades

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching trades for {proxy_wallet}: {e}")
            return []

    def fetch_user_activity(self, proxy_wallet: str, limit: int = 1000) -> List[Dict]:
        """
        Fetch detailed activity for a specific user
        """
        try:
            url = f"{self.data_api_url}/activity"
            params = {
                "user": proxy_wallet,
                "limit": limit,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC"
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                activities = response.json()

                if activities:
                    self._store_user_activity(proxy_wallet, activities)
                    return activities

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching activity for {proxy_wallet}: {e}")
            return []

    def fetch_user_portfolio_value(self, proxy_wallet: str, market_id: str = None) -> Optional[float]:
        """
        Fetch portfolio value for a user (total or for specific market)
        """
        try:
            url = f"{self.data_api_url}/value"
            params = {"user": proxy_wallet}

            if market_id:
                params["market"] = market_id

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                value_data = response.json()

                if value_data and len(value_data) > 0:
                    value = value_data[0].get('value', 0)
                    self._store_user_value(proxy_wallet, value, market_id)
                    return value

            return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching value for {proxy_wallet}: {e}")
            return None

    def fetch_user_closed_positions(self, proxy_wallet: str, limit: int = 50) -> List[Dict]:
        """
        Fetch closed positions for a user
        """
        try:
            url = f"{self.data_api_url}/closed-positions"
            params = {
                "user": proxy_wallet,
                "limit": limit,
                "sortBy": "REALIZEDPNL",
                "sortDirection": "DESC"
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                closed_positions = response.json()

                if closed_positions:
                    self._store_closed_positions(proxy_wallet, closed_positions)
                    return closed_positions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching closed positions for {proxy_wallet}: {e}")
            return []

    def fetch_user_trading_stats(self, proxy_wallet: str) -> Optional[int]:
        """
        Fetch trading statistics for a user (markets traded)
        """
        try:
            url = f"{self.data_api_url}/traded"
            params = {"user": proxy_wallet}

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                data = response.json()

                if data:
                    markets_traded = data.get('traded', 0)
                    self._update_user_stats(proxy_wallet, markets_traded)
                    return markets_traded

            return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching trading stats for {proxy_wallet}: {e}")
            return None

    def identify_whale_users(self) -> List[str]:
        """
        Identify whale users based on portfolio value or transaction history
        """
        # First, identify whales by current portfolio value
        value_whales = self.fetch_all("""
            SELECT DISTINCT proxy_wallet
            FROM user_positions_current
            GROUP BY proxy_wallet
            HAVING SUM(current_value) >= ?
        """, (self.config.MIN_WHALE_WALLET,))

        # Also identify whales by transaction history
        tx_whales = self.fetch_all("""
            SELECT DISTINCT proxy_wallet
            FROM transactions
            WHERE usdc_size >= ?
            GROUP BY proxy_wallet
            HAVING SUM(usdc_size) >= ?
        """, (self.config.MIN_TRANSACTION_SIZE, self.config.MIN_WHALE_WALLET))

        # Combine and deduplicate
        whale_wallets = set()
        for whale in value_whales:
            whale_wallets.add(whale['proxy_wallet'])
        for whale in tx_whales:
            whale_wallets.add(whale['proxy_wallet'])

        # Mark these users as whales in the database
        for wallet in whale_wallets:
            self.update_record(
                'users',
                {'is_whale': 1},
                'proxy_wallet = ?',
                (wallet,)
            )

        self.logger.info(f"Identified {len(whale_wallets)} whale users")
        return list(whale_wallets)

    def fetch_top_holders_for_markets(self, limit_markets: int = 100, min_balance: float = None):
        """
        Fetch top holders for the top markets by volume
        This is optimized for initial data loading
        """
        min_balance = min_balance or self.config.MIN_TRANSACTION_SIZE

        # Get top markets by volume
        top_markets = self.fetch_all("""
            SELECT id, condition_id, question, volume
            FROM markets
            WHERE active = 1 AND condition_id IS NOT NULL
            ORDER BY volume DESC
            LIMIT ?
        """, (limit_markets,))

        self.logger.info(f"Fetching holders for top {len(top_markets)} markets...")

        total_holders = 0
        whale_holders = []

        for idx, market in enumerate(top_markets, 1):
            market_id = market['id']
            condition_id = market['condition_id']
            question = market['question'][:50] if market['question'] else 'Unknown'

            try:
                # Use condition_id as market identifier
                market_hash = condition_id if condition_id.startswith('0x') else condition_id

                url = f"{self.data_api_url}/holders"
                params = {
                    "market": market_hash,
                    "minBalance": min_balance,
                    "limit": 100  # Get top 100 holders per market
                }

                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    holders_data = response.json()

                    for token_group in holders_data:
                        token_id = token_group.get('token')
                        holders = token_group.get('holders', [])

                        # Filter for whale holders only
                        for holder in holders:
                            amount = holder.get('amount', 0)

                            # Only store holders with significant positions
                            if amount >= min_balance:
                                holder_record = {
                                    'market_id': market_id,
                                    'token_id': token_id,
                                    'proxy_wallet': holder.get('proxyWallet'),
                                    'username': holder.get('name'),
                                    'pseudonym': holder.get('pseudonym'),
                                    'amount': amount,
                                    'outcome_index': holder.get('outcomeIndex'),
                                    'bio': holder.get('bio'),
                                    'profile_image': holder.get('profileImage'),
                                    'updated_at': datetime.now().isoformat()
                                }

                                # Store holder
                                self.insert_or_replace('market_holders', holder_record)
                                total_holders += 1

                                # Track whale holders
                                if amount >= self.config.MIN_WHALE_TRADE:
                                    whale_holders.append({
                                        'wallet': holder.get('proxyWallet'),
                                        'market': question,
                                        'amount': amount
                                    })

                                    # Mark as whale user
                                    self.insert_or_ignore('users', {
                                        'proxy_wallet': holder.get('proxyWallet'),
                                        'username': holder.get('name'),
                                        'is_whale': 1
                                    })

                if idx % 10 == 0:
                    self.logger.info(f"Processed {idx}/{len(top_markets)} markets, found {total_holders} holders")

                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)

            except Exception as e:
                self.logger.error(f"Error fetching holders for market {market_id}: {e}")
                continue

        # Sort whale holders by amount
        whale_holders.sort(key=lambda x: x['amount'], reverse=True)

        self.logger.info(f"✅ Fetched {total_holders} total holders")
        self.logger.info(f"🐋 Found {len(whale_holders)} whale holders (>${self.config.MIN_WHALE_TRADE})")

        if whale_holders[:5]:
            self.logger.info("Top 5 whale holders:")
            for whale in whale_holders[:5]:
                self.logger.info(f"  {whale['wallet'][:10]}... - {whale['market']} - Amount: {whale['amount']:,.2f}")

        return total_holders

    def fetch_market_holders(self, market_id: str, condition_id: str = None,
                           min_balance: float = None, limit: int = 100) -> List[Dict]:
        """
        Fetch top holders for a specific market (focusing on whales)
        """
        min_balance = min_balance or self.config.MIN_TRANSACTION_SIZE
        """
        Fetch top holders for a specific market (focusing on whales)
        """
        min_balance = min_balance or self.config.MIN_TRANSACTION_SIZE
        """
        Fetch top holders for a specific market
        """
        try:
            url = f"{self.data_api_url}/holders"

            # Use condition_id if provided, otherwise use market_id
            market_identifier = condition_id or market_id

            params = {
                "market": market_identifier,
                "minBalance": min_balance,
                "limit": limit
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                holders_data = response.json()
                all_holders = []

                for token_group in holders_data:
                    token_id = token_group.get('token')
                    holders = token_group.get('holders', [])

                    for holder in holders:
                        holder['token_id'] = token_id
                        all_holders.append(holder)

                    self._store_market_holders(market_id, token_id, holders)

                return all_holders

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching holders for market {market_id}: {e}")
            return []

    def _store_user_positions(self, proxy_wallet: str, positions: List[Dict]):
        """Store user's current positions"""
        position_records = []
        total_value = 0

        for position in positions:
            record = {
                'proxy_wallet': proxy_wallet,
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
                'end_date': position.get('endDate'),
                'updated_at': datetime.now().isoformat()
            }
            position_records.append(record)
            total_value += position.get('currentValue', 0)

        # Bulk insert positions
        if position_records:
            self.bulk_insert_or_replace('user_positions_current', position_records)

        # Update user total value
        self.update_record(
            'users',
            {'total_value': total_value, 'last_updated': datetime.now().isoformat()},
            'proxy_wallet = ?',
            (proxy_wallet,)
        )

        self.logger.debug(f"Stored {len(positions)} positions for user {proxy_wallet}")

    def _store_user_trades(self, proxy_wallet: str, trades: List[Dict]):
        """Store user's trades"""
        trade_records = []

        for trade in trades:
            record = {
                'proxy_wallet': proxy_wallet,
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
            }
            trade_records.append(record)

            # Update user profile info if present
            if trade.get('name'):
                self._update_user_profile(proxy_wallet, trade)

        # Bulk insert trades
        if trade_records:
            # Use INSERT OR IGNORE to avoid duplicates
            for record in trade_records:
                self.insert_or_ignore('user_trades', record)

        self.logger.debug(f"Stored {len(trades)} trades for user {proxy_wallet}")

    def _store_user_activity(self, proxy_wallet: str, activities: List[Dict]):
        """Store user's activity"""
        activity_records = []

        for activity in activities:
            record = {
                'proxy_wallet': proxy_wallet,
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
            }
            activity_records.append(record)

        # Bulk insert activities
        if activity_records:
            for record in activity_records:
                self.insert_or_ignore('user_activity', record)

        self.logger.debug(f"Stored {len(activities)} activities for user {proxy_wallet}")

    def _store_user_value(self, proxy_wallet: str, value: float, market_id: str = None):
        """Store user portfolio value"""
        record = {
            'proxy_wallet': proxy_wallet,
            'market_condition_id': market_id,
            'value': value,
            'timestamp': datetime.now().isoformat()
        }

        self.insert_or_replace('user_values', record)

        # Update user total value if it's the overall portfolio value
        if not market_id:
            self.update_record(
                'users',
                {'total_value': value, 'last_updated': datetime.now().isoformat()},
                'proxy_wallet = ?',
                (proxy_wallet,)
            )

        self.logger.debug(f"Stored value ${value:,.2f} for user {proxy_wallet}")

    def _store_closed_positions(self, proxy_wallet: str, positions: List[Dict]):
        """Store user's closed positions"""
        position_records = []
        total_pnl = 0

        for position in positions:
            record = {
                'proxy_wallet': proxy_wallet,
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
                'end_date': position.get('endDate'),
                'closed_at': datetime.now().isoformat()
            }
            position_records.append(record)
            total_pnl += position.get('realizedPnl', 0)

        # Bulk insert closed positions
        if position_records:
            for record in position_records:
                self.insert_or_ignore('user_positions_closed', record)

        # Update user total P&L
        self.update_record(
            'users',
            {'total_pnl': total_pnl},
            'proxy_wallet = ?',
            (proxy_wallet,)
        )

        self.logger.debug(f"Stored {len(positions)} closed positions for user {proxy_wallet}")

    def _update_user_stats(self, proxy_wallet: str, markets_traded: int):
        """Update user trading statistics"""
        self.update_record(
            'users',
            {'markets_traded': markets_traded, 'last_updated': datetime.now().isoformat()},
            'proxy_wallet = ?',
            (proxy_wallet,)
        )

        # Mark as whale if trading in many markets
        if markets_traded > 100:
            self.update_record(
                'users',
                {'is_whale': 1},
                'proxy_wallet = ?',
                (proxy_wallet,)
            )

        self.logger.debug(f"Updated stats for user {proxy_wallet}: {markets_traded} markets traded")

    def _store_market_holders(self, market_id: str, token_id: str, holders: List[Dict]):
        """Store market holders"""
        holder_records = []

        for holder in holders:
            record = {
                'market_id': market_id,
                'token_id': token_id,
                'proxy_wallet': holder.get('proxyWallet'),
                'username': holder.get('name'),
                'pseudonym': holder.get('pseudonym'),
                'amount': holder.get('amount'),
                'outcome_index': holder.get('outcomeIndex'),
                'bio': holder.get('bio'),
                'profile_image': holder.get('profileImage'),
                'updated_at': datetime.now().isoformat()
            }
            holder_records.append(record)

            # Update user profile if present
            if holder.get('name'):
                self._update_user_profile(holder.get('proxyWallet'), holder)

        # Bulk insert holders
        if holder_records:
            self.bulk_insert_or_replace('market_holders', holder_records)

        self.logger.debug(f"Stored {len(holders)} holders for market {market_id}")

    def _update_user_profile(self, proxy_wallet: str, user_data: Dict):
        """Update user profile information"""
        profile_data = {}

        if user_data.get('name'):
            profile_data['username'] = user_data.get('name')
        if user_data.get('pseudonym'):
            profile_data['pseudonym'] = user_data.get('pseudonym')
        if user_data.get('bio'):
            profile_data['bio'] = user_data.get('bio')
        if user_data.get('profileImage'):
            profile_data['profile_image'] = user_data.get('profileImage')
        if user_data.get('profileImageOptimized'):
            profile_data['profile_image_optimized'] = user_data.get('profileImageOptimized')

        if profile_data:
            # First try to insert, then update if exists
            self.insert_or_ignore('users', {'proxy_wallet': proxy_wallet, **profile_data})
            self.update_record('users', profile_data, 'proxy_wallet = ?', (proxy_wallet,))

    def process_all_whale_users(self, min_value: float = None):
        """
        Process all whale users (high value traders)
        """
        min_value = min_value or self.config.MIN_WHALE_WALLET

        # Get whale users from database
        whales = self.fetch_all("""
            SELECT DISTINCT proxy_wallet 
            FROM users 
            WHERE total_value >= ? OR is_whale = 1
            ORDER BY total_value DESC
            LIMIT ?
        """, (min_value, self.config.MAX_TRACKED_WALLETS))

        self.logger.info(f"Processing {len(whales)} whale users (>${min_value} USD)...")

        processed = 0
        errors = 0

        for whale in whales:
            try:
                wallet = whale['proxy_wallet']

                # Fetch all data for this whale
                positions = self.fetch_user_current_positions(wallet)

                # Calculate total position value
                total_value = sum(p.get('currentValue', 0) for p in positions)

                # Only continue if user is a whale
                if total_value < self.config.MIN_WHALE_WALLET:
                    continue

                self.fetch_user_trades(wallet)
                self.fetch_user_activity(wallet)
                self.fetch_user_portfolio_value(wallet)
                self.fetch_user_closed_positions(wallet)
                self.fetch_user_trading_stats(wallet)

                # Mark as whale in database
                self.update_record(
                    'users',
                    {'is_whale': 1, 'total_value': total_value},
                    'proxy_wallet = ?',
                    (wallet,)
                )

                processed += 1

                if processed % 10 == 0:
                    self.logger.info(f"Processed {processed}/{len(whales)} whale users")

                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)

            except Exception as e:
                self.logger.error(f"Error processing whale {whale['proxy_wallet']}: {e}")
                errors += 1

        self.logger.info(f"Whale processing complete. Processed: {processed}, Errors: {errors}")

    def daily_scan(self):
        """
        Perform daily scan for user updates
        """
        self.logger.info("Starting daily user scan...")

        # Process whale users
        self.process_all_whale_users()

        # Get active users from recent trades
        active_users = self.fetch_all("""
            SELECT DISTINCT proxy_wallet
            FROM user_trades
            WHERE timestamp > datetime('now', '-7 days')
            LIMIT 100
        """)

        self.logger.info(f"Updating {len(active_users)} active users...")

        for user in active_users:
            wallet = user['proxy_wallet']
            self.fetch_user_current_positions(wallet)
            self.fetch_user_portfolio_value(wallet)
            time.sleep(self.config.RATE_LIMIT_DELAY)

        self.logger.info("Daily user scan complete")

        return len(active_users)