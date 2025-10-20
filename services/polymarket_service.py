import aiohttp
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json
from config import settings


class PolymarketService:
    """Service for interacting with Polymarket API"""

    def __init__(self):
        self.base_url = "https://gamma-api.polymarket.com"
        self.clob_url = "https://clob.polymarket.com"
        self.session = None

    async def _ensure_session(self):
        """Ensure aiohttp session exists"""
        if not self.session:
            self.session = aiohttp.ClientSession()

    async def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make async HTTP request with error handling"""
        await self._ensure_session()
        try:
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"API request failed: {response.status} - {url}")
                    return {}
        except Exception as e:
            print(f"Request error: {e}")
            return {}

    async def get_markets(self, active: bool = True, limit: int = 100) -> List[Dict]:
        """Fetch markets from Polymarket"""
        url = f"{self.base_url}/markets"
        params = {
            "active": active,
            "limit": limit,
            "order": "volume24hr",
            "ascending": False
        }

        response = await self._make_request(url, params)
        return response if isinstance(response, list) else []

    async def get_market_by_id(self, market_id: str) -> Dict:
        """Get detailed market information"""
        url = f"{self.base_url}/markets/{market_id}"
        return await self._make_request(url)

    async def get_market_trades(self, market_id: str, min_amount: float = 1000) -> List[Dict]:
        """Get recent trades for a specific market"""
        url = f"{self.clob_url}/trades"
        params = {
            "market": market_id,
            "limit": 100
        }

        trades = await self._make_request(url, params)

        # Filter for large trades
        if isinstance(trades, list):
            large_trades = []
            for trade in trades:
                try:
                    # Calculate USD value (assuming price and size are in the response)
                    size = float(trade.get('size', 0))
                    price = float(trade.get('price', 0))
                    usd_value = size * price

                    if usd_value >= min_amount:
                        trade['usd_value'] = usd_value
                        large_trades.append(trade)
                except (ValueError, TypeError):
                    continue
            return large_trades
        return []

    async def get_market_orderbook(self, market_id: str) -> Dict:
        """Get orderbook for a market"""
        url = f"{self.clob_url}/book"
        params = {"market": market_id}
        return await self._make_request(url, params)

    async def get_user_positions(self, address: str) -> List[Dict]:
        """Get all positions for a specific wallet address"""
        # Note: This endpoint might require authentication or might not be directly available
        # You may need to aggregate this from trades or use a different approach
        url = f"{self.gamma_url}/users/{address}/positions"
        return await self._make_request(url)

    async def get_recent_large_trades(self, min_amount: float = 1000, limit: int = 200) -> List[Dict]:
        """Get recent large trades across all markets"""
        # First, get active markets
        markets = await self.get_markets(active=True, limit=50)

        all_large_trades = []
        tasks = []

        # Create tasks for fetching trades from each market
        for market in markets[:20]:  # Limit to top 20 markets to avoid rate limiting
            market_id = market.get('id', market.get('market_id', ''))
            if market_id:
                tasks.append(self._get_market_trades_with_info(market_id, market, min_amount))

        # Execute all tasks concurrently
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if not isinstance(result, Exception) and result:
                    all_large_trades.extend(result)

        # Sort by timestamp (most recent first)
        all_large_trades.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        return all_large_trades[:limit]

    async def _get_market_trades_with_info(self, market_id: str, market_info: Dict, min_amount: float) -> List[Dict]:
        """Helper to get trades with market information attached"""
        trades = await self.get_market_trades(market_id, min_amount)

        # Add market information to each trade
        for trade in trades:
            trade['market_info'] = {
                'id': market_id,
                'question': market_info.get('question', 'Unknown'),
                'market_slug': market_info.get('market_slug', ''),
                'volume': market_info.get('volume', 0),
                'liquidity': market_info.get('liquidity', 0)
            }

        return trades

    async def identify_whales(self, min_total_volume: float = 10000, days_back: int = 7) -> List[str]:
        """Identify whale wallets based on trading volume"""
        large_trades = await self.get_recent_large_trades(min_amount=1000, limit=500)

        wallet_volumes = {}

        for trade in large_trades:
            # Extract wallet addresses from trades
            maker = trade.get('maker', '')
            taker = trade.get('taker', '')
            usd_value = trade.get('usd_value', 0)

            if maker:
                wallet_volumes[maker] = wallet_volumes.get(maker, 0) + usd_value
            if taker:
                wallet_volumes[taker] = wallet_volumes.get(taker, 0) + usd_value

        # Filter wallets with significant volume
        whales = [
            wallet for wallet, volume in wallet_volumes.items()
            if volume >= min_total_volume
        ]

        return whales

    async def get_wallet_trade_history(self, wallet_address: str, limit: int = 100) -> List[Dict]:
        """Get trade history for a specific wallet"""
        # Note: This might require different API endpoints or authentication
        # Placeholder implementation
        url = f"{self.clob_url}/trades"
        params = {
            "maker": wallet_address,
            "limit": limit
        }

        maker_trades = await self._make_request(url, params)

        params = {
            "taker": wallet_address,
            "limit": limit
        }

        taker_trades = await self._make_request(url, params)

        all_trades = []
        if isinstance(maker_trades, list):
            all_trades.extend(maker_trades)
        if isinstance(taker_trades, list):
            all_trades.extend(taker_trades)

        return all_trades

    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()