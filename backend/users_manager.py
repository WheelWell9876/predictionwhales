"""
Users Manager for Polymarket Terminal - WHALE FOCUSED
Handles fetching high-value users (whales) and their complete activity profiles with multithreading support
Focused on users with $1000+ wallets or $250+ positions
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager

class UsersManager(DatabaseManager):
    """Manager for whale user operations with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from .config import Config
        self.config = Config
        self.data_api_url = Config.DATA_API_URL
        self.base_url = Config.GAMMA_API_URL
        self.clob_url = Config.CLOB_API_URL
        
        # Whale thresholds
        self.MIN_WALLET_VALUE = 1000  # $1000 minimum wallet value
        self.MIN_POSITION_VALUE = 250  # $250 minimum position value
        self.TOP_HOLDERS_PER_MARKET = 25  # Top 25 holders per market
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters and collections
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._whale_wallets = set()

    # ==================== PHASE 1: FETCH TOP HOLDERS FROM MARKETS ====================
    

    
    def _fetch_and_filter_market_holders_thread_safe(self, market: Dict, total_markets: int) -> Set[str]:
        """
        Thread-safe wrapper for fetching and filtering market holders
        """
        try:
            whale_wallets = self._fetch_and_filter_market_holders(market['id'], market['condition_id'])
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0:
                    self.logger.info(f"  Processed {self._progress_counter}/{total_markets} markets, found {len(self._whale_wallets)} unique whales")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
            return whale_wallets
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e
    

    
    def _check_whale_criteria_from_holder(self, holder: Dict, market_id: str) -> tuple[bool, Optional[Dict]]:
        """
        Check if holder meets whale criteria:
        - $1000+ wallet value OR
        - $250+ position value
        
        Returns: (is_whale: bool, user_data: Dict or None)
        """
        proxy_wallet = holder.get('proxyWallet')
        
        # First check: Get wallet value
        wallet_value = self._fetch_user_wallet_value(proxy_wallet)
        
        # Second check: Check position value
        # Get current market price for this outcome
        position_shares = holder.get('amount', 0)
        
        # Estimate position value (shares typically trade between $0.01 and $0.99)
        # A conservative estimate: if they have >250 shares, position likely >$250
        estimated_position_value = position_shares * 0.5  # Conservative $0.50 per share estimate
        
        # Check whale criteria
        is_whale = (
            wallet_value >= self.MIN_WALLET_VALUE or 
            estimated_position_value >= self.MIN_POSITION_VALUE or
            (wallet_value >= 500 and position_shares >= 100)  # Medium wallet + significant position
        )
        
        if not is_whale:
            return False, None
        
        # Prepare user record
        user_data = {
            'proxy_wallet': proxy_wallet,
            'username': holder.get('name'),
            'pseudonym': holder.get('pseudonym'),
            'bio': holder.get('bio'),
            'profile_image': holder.get('profileImage'),
            'profile_image_optimized': holder.get('profileImageOptimized'),
            'total_value': wallet_value,
            'is_whale': 1,
            'last_updated': datetime.now().isoformat(),
            'created_at': datetime.now().isoformat()
        }
        
        return True, user_data
    



    

    

    

    

    

    

    

    
    # ==================== STORAGE METHODS ====================
    

    



    
    # ==================== BATCH OPERATIONS ====================
    
    def batch_enrich_whales(self, wallet_addresses: List[str]) -> Dict[str, int]:
        """
        Enrich multiple whale users in parallel
        
        Args:
            wallet_addresses: List of wallet addresses to enrich
            
        Returns:
            Dictionary with enrichment statistics
        """
        self.logger.info(f"Batch enriching {len(wallet_addresses)} whale users...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_wallet = {
                executor.submit(self._enrich_single_whale_thread_safe, wallet, len(wallet_addresses)): wallet 
                for wallet in wallet_addresses
            }
            
            for future in as_completed(future_to_wallet):
                wallet = future_to_wallet[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error enriching whale {wallet}: {e}")
        
        return {
            'total_enriched': self._progress_counter,
            'errors': self._error_counter
        }
    
    # ==================== LEGACY/COMPATIBILITY METHODS ====================
    
    def fetch_top_holders_for_markets(self, limit_markets: int = 100) -> int:
        """Legacy method - calls new whale-focused method"""
        result = self.fetch_top_holders_for_all_markets()
        return result['total_whales_found']
    
    def identify_whale_users(self) -> List[str]:
        """Get list of all whale wallet addresses"""
        whales = self.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        return [w['proxy_wallet'] for w in whales]