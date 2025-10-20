from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import asyncio
from collections import defaultdict
import json


class WhaleTracker:
    """Service for tracking and analyzing whale activity"""

    def __init__(self, polymarket_service):
        self.polymarket = polymarket_service
        self.tracked_wallets = set()
        self.whale_data = {}
        self.recent_bets = []
        self.bet_results = {}
        self.last_update = None

    async def update_whale_data(self):
        """Main update function to refresh whale data"""
        print("Updating whale data...")

        # Identify new whales
        new_whales = await self.polymarket.identify_whales(min_total_volume=5000)
        for whale in new_whales:
            self.tracked_wallets.add(whale)

        # Update data for tracked wallets
        tasks = []
        for wallet in list(self.tracked_wallets)[:50]:  # Limit to avoid rate limiting
            tasks.append(self._update_wallet_data(wallet))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Update recent large bets
        await self._update_recent_bets()

        self.last_update = datetime.now()
        print(f"Whale data updated. Tracking {len(self.tracked_wallets)} wallets")

    async def _update_wallet_data(self, wallet_address: str):
        """Update data for a specific wallet"""
        try:
            trades = await self.polymarket.get_wallet_trade_history(wallet_address)

            if wallet_address not in self.whale_data:
                self.whale_data[wallet_address] = {
                    'address': wallet_address,
                    'total_volume': 0,
                    'total_bets': 0,
                    'winning_bets': 0,
                    'losing_bets': 0,
                    'pending_bets': 0,
                    'profit_loss': 0,
                    'largest_bet': 0,
                    'recent_trades': [],
                    'markets_traded': set(),
                    'last_activity': None,
                    'win_rate': 0
                }

            whale_info = self.whale_data[wallet_address]

            for trade in trades:
                # Calculate trade value
                try:
                    size = float(trade.get('size', 0))
                    price = float(trade.get('price', 0))
                    trade_value = size * price

                    whale_info['total_volume'] += trade_value
                    whale_info['total_bets'] += 1

                    if trade_value > whale_info['largest_bet']:
                        whale_info['largest_bet'] = trade_value

                    # Track markets
                    market_id = trade.get('market', trade.get('market_id', ''))
                    if market_id:
                        whale_info['markets_traded'].add(market_id)

                    # Update last activity
                    trade_time = trade.get('timestamp', '')
                    if trade_time:
                        whale_info['last_activity'] = trade_time

                    # Add to recent trades (keep last 20)
                    whale_info['recent_trades'].append({
                        'market_id': market_id,
                        'side': trade.get('side', ''),
                        'price': price,
                        'size': size,
                        'value': trade_value,
                        'timestamp': trade_time
                    })
                    whale_info['recent_trades'] = whale_info['recent_trades'][-20:]

                except (ValueError, TypeError):
                    continue

            # Convert set to list for JSON serialization
            whale_info['markets_traded'] = list(whale_info['markets_traded'])

            # Calculate win rate
            total_resolved = whale_info['winning_bets'] + whale_info['losing_bets']
            if total_resolved > 0:
                whale_info['win_rate'] = whale_info['winning_bets'] / total_resolved

        except Exception as e:
            print(f"Error updating wallet {wallet_address}: {e}")

    async def _update_recent_bets(self):
        """Update the list of recent large bets"""
        try:
            large_trades = await self.polymarket.get_recent_large_trades(min_amount=1000, limit=100)

            self.recent_bets = []
            for trade in large_trades:
                bet_info = {
                    'id': trade.get('id', ''),
                    'market': trade.get('market_info', {}),
                    'maker': trade.get('maker', ''),
                    'taker': trade.get('taker', ''),
                    'side': trade.get('side', ''),
                    'price': float(trade.get('price', 0)),
                    'size': float(trade.get('size', 0)),
                    'value': float(trade.get('usd_value', 0)),
                    'timestamp': trade.get('timestamp', ''),
                    'is_whale': False
                }

                # Check if it's a whale trade
                if bet_info['maker'] in self.tracked_wallets or bet_info['taker'] in self.tracked_wallets:
                    bet_info['is_whale'] = True

                self.recent_bets.append(bet_info)

        except Exception as e:
            print(f"Error updating recent bets: {e}")

    async def get_whale_stats(self) -> List[Dict]:
        """Get statistics for all tracked whales"""
        whale_list = []

        for wallet_address, data in self.whale_data.items():
            whale_summary = {
                'address': wallet_address,
                'total_volume': round(data['total_volume'], 2),
                'total_bets': data['total_bets'],
                'win_rate': round(data['win_rate'] * 100, 2),
                'profit_loss': round(data['profit_loss'], 2),
                'largest_bet': round(data['largest_bet'], 2),
                'markets_traded': len(data['markets_traded']),
                'last_activity': data['last_activity']
            }
            whale_list.append(whale_summary)

        # Sort by total volume
        whale_list.sort(key=lambda x: x['total_volume'], reverse=True)

        return whale_list

    async def get_wallet_details(self, wallet_address: str) -> Optional[Dict]:
        """Get detailed information about a specific wallet"""
        if wallet_address not in self.whale_data:
            # Try to fetch data if not tracked
            await self._update_wallet_data(wallet_address)

        if wallet_address in self.whale_data:
            return self.whale_data[wallet_address]

        return None

    async def get_recent_large_bets(self, min_amount: float = 1000) -> List[Dict]:
        """Get recent large bets filtered by minimum amount"""
        filtered_bets = [
            bet for bet in self.recent_bets
            if bet['value'] >= min_amount
        ]

        return filtered_bets

    async def get_active_markets_with_whale_activity(self) -> List[Dict]:
        """Get markets with significant whale activity"""
        market_activity = defaultdict(lambda: {
            'whale_count': 0,
            'total_whale_volume': 0,
            'recent_whale_trades': []
        })

        # Aggregate whale activity by market
        for wallet_address, data in self.whale_data.items():
            for trade in data.get('recent_trades', []):
                market_id = trade.get('market_id', '')
                if market_id:
                    market_activity[market_id]['whale_count'] += 1
                    market_activity[market_id]['total_whale_volume'] += trade.get('value', 0)
                    market_activity[market_id]['recent_whale_trades'].append({
                        'wallet': wallet_address,
                        'value': trade.get('value', 0),
                        'side': trade.get('side', ''),
                        'timestamp': trade.get('timestamp', '')
                    })

        # Get market details and combine with activity
        active_markets = []
        for market_id, activity in market_activity.items():
            if activity['whale_count'] > 0:
                market_info = await self.polymarket.get_market_by_id(market_id)

                active_markets.append({
                    'market_id': market_id,
                    'question': market_info.get('question', 'Unknown'),
                    'whale_count': activity['whale_count'],
                    'total_whale_volume': round(activity['total_whale_volume'], 2),
                    'recent_trades': activity['recent_whale_trades'][:5]  # Last 5 trades
                })

        # Sort by whale volume
        active_markets.sort(key=lambda x: x['total_whale_volume'], reverse=True)

        return active_markets[:20]  # Top 20 markets

    async def add_wallet_to_track(self, wallet_address: str) -> bool:
        """Manually add a wallet to track"""
        if wallet_address not in self.tracked_wallets:
            self.tracked_wallets.add(wallet_address)
            await self._update_wallet_data(wallet_address)
            return True
        return False

    async def fetch_wallet_history(self, wallet_address: str):
        """Fetch complete history for a wallet (background task)"""
        await self._update_wallet_data(wallet_address)

    async def get_summary_stats(self) -> Dict:
        """Get summary statistics across all whales"""
        total_whales = len(self.tracked_wallets)
        total_volume = sum(w.get('total_volume', 0) for w in self.whale_data.values())
        total_bets = sum(w.get('total_bets', 0) for w in self.whale_data.values())

        avg_win_rate = 0
        if total_whales > 0:
            win_rates = [w.get('win_rate', 0) for w in self.whale_data.values()]
            avg_win_rate = sum(win_rates) / len(win_rates) * 100

        return {
            'total_whales_tracked': total_whales,
            'total_volume_tracked': round(total_volume, 2),
            'total_bets_tracked': total_bets,
            'average_win_rate': round(avg_win_rate, 2),
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'recent_large_bets_count': len(self.recent_bets)
        }