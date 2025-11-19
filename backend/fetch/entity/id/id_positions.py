"""
ID positions
Handles individual fetching for the positions
"""

from threading import Lock
from typing import Dict, List
import requests
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_positions import StorePositionsManager

class IdPositionsManager(DatabaseManager):
    """Manager for individual position fetching"""
    
    def __init__(self):
        super().__init__()
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        self._lock = Lock()  # Thread-safe database operations
        self.store_manager = StorePositionsManager()
        
        # Set max workers
        self.max_workers = min(20, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 20))
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        
        # Position thresholds
        self.MIN_POSITION_VALUE = 500  # Minimum position value to track

    def fetch_user_current_positions(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user's current positions"""
        try:
            url = f"{self.data_api_url}/positions"
            params = {"user": proxy_wallet, "status": "ACTIVE"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                positions = response.json() or []
                if positions:
                    with self._lock:
                        self.store_manager._store_user_current_positions(proxy_wallet, positions)
                return positions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching current positions for {proxy_wallet}: {e}")
            return []

    def fetch_user_closed_positions(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user's closed positions"""
        try:
            url = f"{self.data_api_url}/positions"
            params = {"user": proxy_wallet, "status": "CLOSED", "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                positions = response.json() or []
                if positions:
                    with self._lock:
                        self.store_manager._store_user_closed_positions(proxy_wallet, positions)
                return positions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching closed positions for {proxy_wallet}: {e}")
            return []

    def _fetch_user_positions_api(self, proxy_wallet: str) -> Dict:
        """Fetch current positions for a single user from API (internal use)"""
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
        """Fetch closed positions for a single user from API (internal use)"""
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