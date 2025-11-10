"""
Transactions Manager for Polymarket Terminal
Handles fetching, processing, and storing transaction data from CLOB API
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from .database_manager import DatabaseManager

class TransactionsManager(DatabaseManager):
    """Manager for transaction-related operations"""

    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.clob_api_url = Config.CLOB_API_URL if Config.CLOB_API_URL else "https://clob.polymarket.com"
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"

    def fetch_recent_transactions(self, market_id: str = None, limit: int = 100) -> List[Dict]:
        """
        Fetch recent transactions from CLOB API (only whale transactions)
        """
        try:
            url = f"{self.clob_api_url}/trades"
            params = {
                "limit": limit,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC",
                "minSize": self.config.MIN_TRANSACTION_SIZE  # Only get whale transactions
            }

            if market_id:
                params["market"] = market_id

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                transactions = response.json()

                # Filter for whale transactions only
                whale_transactions = []
                for tx in transactions:
                    usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
                    if usdc_size >= self.config.MIN_TRANSACTION_SIZE:
                        tx['is_whale'] = 1 if usdc_size >= self.config.MIN_WHALE_TRADE else 0
                        whale_transactions.append(tx)

                if whale_transactions:
                    self._store_transactions(whale_transactions)
                    return whale_transactions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching transactions: {e}")
            return []

    def fetch_whale_transactions(self, min_size: float = None, limit: int = 100) -> List[Dict]:
        """
        Fetch whale transactions (large trades)
        """
        min_size = min_size or self.config.MIN_WHALE_TRADE

        try:
            url = f"{self.clob_api_url}/trades"
            params = {
                "limit": limit,
                "minSize": min_size,
                "sortBy": "SIZE",
                "sortDirection": "DESC"
            }

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                whale_transactions = response.json()

                if whale_transactions:
                    # Mark these as whale transactions
                    for tx in whale_transactions:
                        tx['is_whale'] = 1

                    self._store_transactions(whale_transactions)
                    return whale_transactions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching whale transactions: {e}")
            return []

    def fetch_user_transactions(self, proxy_wallet: str, limit: int = 100) -> List[Dict]:
        """
        Fetch transactions for a specific user
        """
        try:
            url = f"{self.clob_api_url}/trades"
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
                transactions = response.json()

                if transactions:
                    self._store_transactions(transactions)
                    return transactions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching transactions for {proxy_wallet}: {e}")
            return []

    def fetch_market_transactions(self, market_id: str, condition_id: str = None,
                                 limit: int = 100) -> List[Dict]:
        """
        Fetch transactions for a specific market
        """
        try:
            url = f"{self.clob_api_url}/trades"

            # Use condition_id if provided, otherwise use market_id
            market_identifier = condition_id or market_id

            params = {
                "market": market_identifier,
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
                transactions = response.json()

                if transactions:
                    # Add market_id to each transaction
                    for tx in transactions:
                        tx['market_id'] = market_id

                    self._store_transactions(transactions)
                    return transactions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching transactions for market {market_id}: {e}")
            return []

    def fetch_time_range_transactions(self, start_time: datetime, end_time: datetime,
                                     market_id: str = None) -> List[Dict]:
        """
        Fetch transactions within a specific time range
        """
        try:
            url = f"{self.clob_api_url}/trades"
            params = {
                "startTime": start_time.isoformat(),
                "endTime": end_time.isoformat(),
                "limit": 1000,
                "sortBy": "TIMESTAMP",
                "sortDirection": "DESC"
            }

            if market_id:
                params["market"] = market_id

            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                transactions = response.json()

                if transactions:
                    self._store_transactions(transactions)
                    return transactions

            return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching transactions for time range: {e}")
            return []

    def _store_transactions(self, transactions: List[Dict]):
        """Store transactions in database (only whale transactions)"""
        transaction_records = []

        for tx in transactions:
            # Calculate USDC size if not provided
            usdc_size = tx.get('usdcSize') or (tx.get('size', 0) * tx.get('price', 0))

            # Only store transactions above minimum threshold
            if usdc_size < self.config.MIN_TRANSACTION_SIZE:
                continue

            record = {
                'id': tx.get('id') or tx.get('transactionHash'),
                'proxy_wallet': tx.get('proxyWallet') or tx.get('user'),
                'username': tx.get('name') or tx.get('username'),
                'condition_id': tx.get('conditionId'),
                'time_created': tx.get('timestamp') or tx.get('createdAt'),
                'usdc_size': usdc_size,
                'shares_count': tx.get('size'),
                'avg_price': tx.get('price'),
                'side': tx.get('side'),
                'type': tx.get('type') or 'TRADE',
                'market_id': tx.get('market_id') or tx.get('marketId'),
                'transaction_hash': tx.get('transactionHash'),
                'outcome': tx.get('outcome'),
                'outcome_index': tx.get('outcomeIndex'),
                'is_whale': 1 if usdc_size >= self.config.MIN_WHALE_TRADE else 0
            }
            transaction_records.append(record)

            # Update user info if present and is whale
            if tx.get('proxyWallet') and usdc_size >= self.config.MIN_TRANSACTION_SIZE:
                self._update_user_from_transaction(tx)

        # Bulk insert transactions
        if transaction_records:
            for record in transaction_records:
                self.insert_or_ignore('transactions', record)
            self.logger.info(f"Stored {len(transaction_records)} whale transactions (>${self.config.MIN_TRANSACTION_SIZE} USD)")

        self.logger.debug(f"Filtered {len(transactions) - len(transaction_records)} small transactions")

    def _update_user_from_transaction(self, tx: Dict):
        """Update user information from transaction data"""
        proxy_wallet = tx.get('proxyWallet') or tx.get('user')
        if not proxy_wallet:
            return

        user_data = {'proxy_wallet': proxy_wallet}

        if tx.get('name') or tx.get('username'):
            user_data['username'] = tx.get('name') or tx.get('username')

        # Mark as whale if large transaction
        usdc_size = tx.get('usdcSize', 0) or (tx.get('size', 0) * tx.get('price', 0))
        if usdc_size >= 10000:
            user_data['is_whale'] = 1

        # Insert or update user
        self.insert_or_ignore('users', user_data)

        if 'username' in user_data:
            self.update_record(
                'users',
                {'username': user_data['username']},
                'proxy_wallet = ?',
                (proxy_wallet,)
            )

    def analyze_transaction_flow(self, market_id: str = None, hours: int = 24) -> Dict:
        """
        Analyze transaction flow for a market or overall
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        query = """
            SELECT 
                COUNT(*) as total_trades,
                SUM(usdc_size) as total_volume,
                AVG(usdc_size) as avg_trade_size,
                MAX(usdc_size) as largest_trade,
                SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END) as buy_volume,
                SUM(CASE WHEN side = 'SELL' THEN usdc_size ELSE 0 END) as sell_volume,
                COUNT(DISTINCT proxy_wallet) as unique_traders,
                SUM(CASE WHEN is_whale = 1 THEN 1 ELSE 0 END) as whale_trades
            FROM transactions
            WHERE time_created >= ? AND time_created <= ?
        """

        params = [start_time.isoformat(), end_time.isoformat()]

        if market_id:
            query += " AND market_id = ?"
            params.append(market_id)

        result = self.fetch_one(query, tuple(params))

        if result:
            analysis = {
                'period_hours': hours,
                'total_trades': result['total_trades'] or 0,
                'total_volume': result['total_volume'] or 0,
                'avg_trade_size': result['avg_trade_size'] or 0,
                'largest_trade': result['largest_trade'] or 0,
                'buy_volume': result['buy_volume'] or 0,
                'sell_volume': result['sell_volume'] or 0,
                'unique_traders': result['unique_traders'] or 0,
                'whale_trades': result['whale_trades'] or 0,
                'buy_sell_ratio': (result['buy_volume'] or 0) / (result['sell_volume'] or 1)
            }

            return analysis

        return {}

    def get_whale_activity(self, hours: int = 24) -> List[Dict]:
        """
        Get recent whale activity
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)

        whales = self.fetch_all("""
            SELECT 
                t.*,
                u.username,
                u.total_value,
                m.question as market_question
            FROM transactions t
            LEFT JOIN users u ON t.proxy_wallet = u.proxy_wallet
            LEFT JOIN markets m ON t.market_id = m.id
            WHERE t.is_whale = 1
            AND t.time_created >= ?
            AND t.time_created <= ?
            ORDER BY t.usdc_size DESC
            LIMIT 100
        """, (start_time.isoformat(), end_time.isoformat()))

        return whales

    def get_market_momentum(self, market_id: str, hours: int = 6) -> Dict:
        """
        Calculate market momentum based on recent transactions
        """
        end_time = datetime.now()

        momentum = {}

        for period_hours in [1, 6, 24]:
            start_time = end_time - timedelta(hours=period_hours)

            result = self.fetch_one("""
                SELECT 
                    SUM(CASE WHEN side = 'BUY' THEN usdc_size ELSE 0 END) as buy_volume,
                    SUM(CASE WHEN side = 'SELL' THEN usdc_size ELSE 0 END) as sell_volume,
                    COUNT(*) as trade_count,
                    COUNT(DISTINCT proxy_wallet) as unique_traders
                FROM transactions
                WHERE market_id = ?
                AND time_created >= ?
                AND time_created <= ?
            """, (market_id, start_time.isoformat(), end_time.isoformat()))

            if result:
                buy_vol = result['buy_volume'] or 0
                sell_vol = result['sell_volume'] or 0

                momentum[f'{period_hours}h'] = {
                    'buy_volume': buy_vol,
                    'sell_volume': sell_vol,
                    'net_flow': buy_vol - sell_vol,
                    'trade_count': result['trade_count'] or 0,
                    'unique_traders': result['unique_traders'] or 0,
                    'momentum_score': (buy_vol - sell_vol) / (buy_vol + sell_vol + 1) * 100
                }

        return momentum

    def process_all_active_markets_transactions(self):
        """
        Process transactions for all active markets
        """
        # Get active markets
        markets = self.fetch_all("""
            SELECT id, condition_id, question
            FROM markets
            WHERE active = 1
            ORDER BY volume DESC
            LIMIT 100
        """)

        self.logger.info(f"Processing transactions for {len(markets)} active markets...")

        processed = 0
        errors = 0

        for market in markets:
            try:
                self.fetch_market_transactions(
                    market['id'],
                    market['condition_id']
                )
                processed += 1

                if processed % 10 == 0:
                    self.logger.info(f"Processed {processed}/{len(markets)} markets")

                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)

            except Exception as e:
                self.logger.error(f"Error processing market {market['id']}: {e}")
                errors += 1

        self.logger.info(f"Market transaction processing complete. Processed: {processed}, Errors: {errors}")

    def daily_scan(self):
        """
        Perform daily scan for transaction updates
        """
        self.logger.info("Starting daily transaction scan...")

        # Fetch recent whale transactions
        self.logger.info("Fetching whale transactions...")
        whale_txs = self.fetch_whale_transactions(min_size=10000, limit=500)
        self.logger.info(f"Found {len(whale_txs)} whale transactions")

        # Process transactions for active markets
        self.process_all_active_markets_transactions()

        # Analyze overall flow
        analysis = self.analyze_transaction_flow(hours=24)
        self.logger.info(f"24h Transaction Analysis:")
        self.logger.info(f"  Total Volume: ${analysis.get('total_volume', 0):,.2f}")
        self.logger.info(f"  Total Trades: {analysis.get('total_trades', 0)}")
        self.logger.info(f"  Unique Traders: {analysis.get('unique_traders', 0)}")
        self.logger.info(f"  Whale Trades: {analysis.get('whale_trades', 0)}")

        self.logger.info("Daily transaction scan complete")

        return len(whale_txs)