import os
import json
import time
import hmac
import base64
import hashlib
from typing import Dict, List, Optional, Any
from datetime import datetime
import aiohttp
import asyncio
from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_defunct
from dotenv import load_dotenv

load_dotenv()


class PolymarketClient:
    """Enhanced Polymarket client with full API integration"""

    def __init__(self):
        # API Endpoints
        self.clob_url = "https://clob.polymarket.com"
        self.gamma_url = "https://gamma-api.polymarket.com"
        self.data_url = "https://data-api.polymarket.com"
        self.strapi_url = "https://strapi-matic.polymarket.com"

        # Web3 setup for Polygon
        self.polygon_rpc = os.getenv('POLYGON_RPC_URL', 'https://polygon-rpc.com')
        self.w3 = Web3(Web3.HTTPProvider(self.polygon_rpc))

        # Wallet setup (observer only - no transactions)
        self.private_key = os.getenv('OBSERVER_PRIVATE_KEY')
        self.address = None
        self.api_key = None
        self.api_secret = None
        self.api_passphrase = None

        if self.private_key:
            self.account = Account.from_key(self.private_key)
            self.address = self.account.address

        self.session = None

    async def initialize(self):
        """Initialize the client and get API credentials"""
        await self._ensure_session()

        # Get API credentials if we have a wallet
        if self.private_key and not self.api_key:
            await self.get_api_credentials()

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def get_api_credentials(self):
        """Get API credentials from Polymarket"""
        try:
            # Create nonce
            nonce = int(time.time() * 1000)

            # Sign the nonce
            message = f"Sign in to Polymarket\nNonce: {nonce}"
            message_hash = encode_defunct(text=message)
            signature = self.account.sign_message(message_hash)

            # Request API keys
            url = f"{self.clob_url}/auth/api-key"
            headers = {
                "Content-Type": "application/json"
            }

            payload = {
                "address": self.address.lower(),
                "signature": signature.signature.hex(),
                "nonce": nonce
            }

            async with self.session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    self.api_key = data.get('apiKey')
                    self.api_secret = data.get('secret')
                    self.api_passphrase = data.get('passphrase')

                    # Save to environment
                    with open('.env', 'a') as f:
                        f.write(f"\nPOLYMARKET_API_KEY={self.api_key}")
                        f.write(f"\nPOLYMARKET_API_SECRET={self.api_secret}")
                        f.write(f"\nPOLYMARKET_API_PASSPHRASE={self.api_passphrase}")

                    print(f"✓ API credentials obtained for {self.address}")
                    return True
                else:
                    print(f"Failed to get API credentials: {response.status}")
                    return False

        except Exception as e:
            print(f"Error getting API credentials: {e}")
            return False

    def _create_signature(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """Create HMAC signature for authenticated requests"""
        if not self.api_secret:
            return ""

        message = f"{timestamp}{method}{path}{body}"
        signature = hmac.new(
            base64.b64decode(self.api_secret),
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()

        return base64.b64encode(signature).decode()

    async def _make_authenticated_request(self, method: str, url: str, data: Optional[Dict] = None) -> Dict:
        """Make authenticated request to CLOB API"""
        await self._ensure_session()

        try:
            timestamp = str(int(time.time() * 1000))
            path = url.replace(self.clob_url, "")
            body = json.dumps(data) if data else ""

            headers = {
                "POLY-ADDRESS": self.address.lower() if self.address else "",
                "POLY-SIGNATURE": self._create_signature(timestamp, method, path, body),
                "POLY-TIMESTAMP": timestamp,
                "POLY-PASSPHRASE": self.api_passphrase or "",
                "Content-Type": "application/json"
            }

            if method == "GET":
                async with self.session.get(url, headers=headers) as response:
                    return await response.json()
            elif method == "POST":
                async with self.session.post(url, json=data, headers=headers) as response:
                    return await response.json()

        except Exception as e:
            print(f"Authenticated request error: {e}")
            return {}

    async def _make_public_request(self, url: str, params: Optional[Dict] = None) -> Any:
        """Make public API request"""
        await self._ensure_session()

        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Request failed: {response.status} - {url}")
                    return {}
        except Exception as e:
            print(f"Request error: {e}")
            return {}

    # CLOB API Endpoints
    async def get_orderbook(self, token_id: str) -> Dict:
        """Get orderbook for a specific market"""
        url = f"{self.clob_url}/book"
        params = {"token_id": token_id}
        return await self._make_public_request(url, params)

    async def get_trades(self, market: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get recent trades"""
        url = f"{self.clob_url}/trades"
        params = {"limit": limit}
        if market:
            params["market"] = market

        return await self._make_public_request(url, params)

    async def get_prices(self, market: str) -> Dict:
        """Get current prices for a market"""
        url = f"{self.clob_url}/price"
        params = {"market": market}
        return await self._make_public_request(url, params)

    async def get_markets_orderbook(self) -> List[Dict]:
        """Get orderbooks for all markets"""
        url = f"{self.clob_url}/books"
        return await self._make_public_request(url)

    # Gamma Markets API
    async def get_markets(self, active: bool = True, closed: bool = False, limit: int = 100) -> List[Dict]:
        """Get markets from Gamma API"""
        url = f"{self.gamma_url}/markets"
        params = {
            "active": active,
            "closed": closed,
            "limit": limit,
            "offset": 0
        }
        return await self._make_public_request(url, params)

    async def get_market(self, condition_id: str) -> Dict:
        """Get specific market details"""
        url = f"{self.gamma_url}/markets/{condition_id}"
        return await self._make_public_request(url)

    async def get_events(self, active: bool = True, closed: bool = False, limit: int = 100) -> List[Dict]:
        """Get events from Gamma API"""
        url = f"{self.gamma_url}/events"
        params = {
            "active": active,
            "closed": closed,
            "limit": limit
        }
        return await self._make_public_request(url, params)

    # Data API Endpoints
    async def get_market_trades_history(self, market_slug: str, limit: int = 100) -> List[Dict]:
        """Get historical trades for a market"""
        url = f"{self.data_url}/markets/{market_slug}/activity"
        params = {"limit": limit}
        return await self._make_public_request(url, params)

    async def get_leaderboard(self, interval: str = "day") -> List[Dict]:
        """Get trading leaderboard"""
        url = f"{self.data_url}/leaderboard"
        params = {"interval": interval}  # day, week, month, all_time
        return await self._make_public_request(url, params)

    async def get_user_positions(self, address: str) -> List[Dict]:
        """Get positions for a specific user"""
        url = f"{self.data_url}/users/{address.lower()}/positions"
        return await self._make_public_request(url)

    async def get_user_history(self, address: str) -> List[Dict]:
        """Get trading history for a user"""
        url = f"{self.data_url}/users/{address.lower()}/history"
        return await self._make_public_request(url)

    # Strapi Content API
    async def get_market_metadata(self, market_slug: str) -> Dict:
        """Get detailed market metadata from Strapi"""
        url = f"{self.strapi_url}/markets/{market_slug}"
        return await self._make_public_request(url)

    # Whale Detection Methods
    async def identify_whales_from_trades(self, min_trade_size: float = 1000, hours_back: int = 24) -> List[Dict]:
        """Identify whales from recent large trades"""
        whale_addresses = {}

        # Get recent markets
        markets = await self.get_markets(active=True, limit=50)

        for market in markets[:20]:  # Limit to avoid rate limiting
            condition_id = market.get('condition_id', '')
            if not condition_id:
                continue

            # Get trades for this market
            trades = await self.get_trades(market=condition_id, limit=200)

            if not isinstance(trades, list):
                continue

            for trade in trades:
                # Parse trade data
                maker = trade.get('maker', '')
                taker = trade.get('taker', '')
                size = float(trade.get('size', 0))
                price = float(trade.get('price', 0))

                # Calculate USD value (assuming USDC denominated)
                usd_value = size * price / 1e6  # Adjust for token decimals

                if usd_value >= min_trade_size:
                    # Track maker
                    if maker:
                        if maker not in whale_addresses:
                            whale_addresses[maker] = {
                                'address': maker,
                                'total_volume': 0,
                                'trade_count': 0,
                                'markets': set(),
                                'largest_trade': 0
                            }
                        whale_addresses[maker]['total_volume'] += usd_value
                        whale_addresses[maker]['trade_count'] += 1
                        whale_addresses[maker]['markets'].add(condition_id)
                        whale_addresses[maker]['largest_trade'] = max(
                            whale_addresses[maker]['largest_trade'],
                            usd_value
                        )

                    # Track taker
                    if taker:
                        if taker not in whale_addresses:
                            whale_addresses[taker] = {
                                'address': taker,
                                'total_volume': 0,
                                'trade_count': 0,
                                'markets': set(),
                                'largest_trade': 0
                            }
                        whale_addresses[taker]['total_volume'] += usd_value
                        whale_addresses[taker]['trade_count'] += 1
                        whale_addresses[taker]['markets'].add(condition_id)
                        whale_addresses[taker]['largest_trade'] = max(
                            whale_addresses[taker]['largest_trade'],
                            usd_value
                        )

            # Small delay to avoid rate limiting
            await asyncio.sleep(0.5)

        # Convert sets to lists and filter by minimum volume
        whales = []
        for address, data in whale_addresses.items():
            if data['total_volume'] >= 5000:  # Min $5k volume to be considered whale
                data['markets'] = list(data['markets'])
                whales.append(data)

        # Sort by total volume
        whales.sort(key=lambda x: x['total_volume'], reverse=True)

        return whales

    async def get_whale_leaderboard(self) -> List[Dict]:
        """Get whales from official leaderboard"""
        intervals = ['day', 'week', 'month']
        all_whales = {}

        for interval in intervals:
            leaderboard = await self.get_leaderboard(interval)

            if isinstance(leaderboard, list):
                for trader in leaderboard[:50]:  # Top 50 traders
                    address = trader.get('user', '')
                    volume = float(trader.get('volume', 0))
                    profit = float(trader.get('profit', 0))

                    if address:
                        if address not in all_whales:
                            all_whales[address] = {
                                'address': address,
                                'volume': 0,
                                'profit': 0,
                                'rankings': {}
                            }

                        all_whales[address]['volume'] = max(all_whales[address]['volume'], volume)
                        all_whales[address]['profit'] = max(all_whales[address]['profit'], profit)
                        all_whales[address]['rankings'][interval] = trader.get('rank', 0)

            await asyncio.sleep(0.5)

        return list(all_whales.values())

    async def monitor_wallet_realtime(self, address: str) -> Dict:
        """Monitor a specific wallet's activity in real-time"""
        wallet_data = {
            'address': address,
            'positions': [],
            'recent_trades': [],
            'total_volume': 0,
            'unrealized_pnl': 0,
            'realized_pnl': 0
        }

        # Get current positions
        positions = await self.get_user_positions(address)
        if isinstance(positions, list):
            wallet_data['positions'] = positions

            # Calculate unrealized PnL
            for position in positions:
                market_price = position.get('market_price', 0)
                avg_price = position.get('avg_price', 0)
                size = position.get('size', 0)
                unrealized = (market_price - avg_price) * size
                wallet_data['unrealized_pnl'] += unrealized

        # Get recent history
        history = await self.get_user_history(address)
        if isinstance(history, list):
            wallet_data['recent_trades'] = history[:20]

            # Calculate volumes and realized PnL
            for trade in history:
                wallet_data['total_volume'] += float(trade.get('volume', 0))
                wallet_data['realized_pnl'] += float(trade.get('profit', 0))

        return wallet_data

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()