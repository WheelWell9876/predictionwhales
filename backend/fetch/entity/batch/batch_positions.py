"""
Batch positions
Handles batch fetching for the positions
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import Dict, List
import requests
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_positions import StorePositionsManager

class BatchPositionsManager(DatabaseManager):
    """Manager for batch position fetching"""
    
    def __init__(self):
        super().__init__()
        self.config = Config
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StorePositionsManager()
        
        # Set max workers
        self.max_workers = min(5, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 5))
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Position thresholds
        self.MIN_POSITION_VALUE = 500  # Minimum position value to track

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
            self.store_manager._bulk_insert_positions(all_positions)
        
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
            self.store_manager._bulk_insert_closed_positions(all_positions)
        
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