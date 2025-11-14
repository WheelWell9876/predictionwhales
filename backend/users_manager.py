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
from .database_manager import DatabaseManager

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
    
    def fetch_top_holders_for_all_markets(self) -> Dict[str, int]:
        """
        Fetch top 25 holders for ALL active markets using multithreading
        Only store users meeting whale criteria ($1000+ wallet OR $250+ position)
        Returns: {'total_markets_processed': X, 'total_whales_found': Y}
        """
        self.logger.info("🐋 Fetching top holders for all active markets with multithreading...")
        
        # Get all active markets
        markets = self.fetch_all("""
            SELECT id, condition_id, question
            FROM markets
            WHERE active = 1 AND condition_id IS NOT NULL
            ORDER BY volume DESC
        """)
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        self._whale_wallets = set()
        
        self.logger.info(f"Processing {len(markets)} markets using {self.max_workers} threads...")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_market = {
                executor.submit(self._fetch_and_filter_market_holders_thread_safe, market, len(markets)): market 
                for market in markets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_market):
                market = future_to_market[future]
                try:
                    whale_wallets = future.result()
                    with self._progress_lock:
                        self._whale_wallets.update(whale_wallets)
                except Exception as e:
                    self.logger.error(f"Error processing market {market['id']}: {e}")
        
        self.logger.info(f"✅ Found {len(self._whale_wallets)} whale users across {self._progress_counter} markets")
        
        return {
            'total_markets_processed': self._progress_counter,
            'total_whales_found': len(self._whale_wallets)
        }
    
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
    
    def _fetch_and_filter_market_holders(self, market_id: str, condition_id: str) -> Set[str]:
        """
        Fetch top holders for a specific market and filter by whale criteria
        Returns set of wallet addresses that meet whale criteria
        """
        try:
            # Fetch market holders using DATA API
            url = f"{self.data_api_url}/holders"
            params = {
                "market": condition_id,
                "minBalance": 1,  # Get all holders with at least 1 share
                "limit": 100  # Get more holders to filter for whales
            }
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                return set()
            
            holders_data = response.json()
            
            if not holders_data:
                return set()
            
            whale_wallets = set()
            holder_records = []
            
            # Process all token groups (YES/NO outcomes)
            for token_group in holders_data:
                token_id = token_group.get('token')
                holders = token_group.get('holders', [])
                outcome_index = token_group.get('outcomeIndex', 0)
                
                # Process each holder
                for holder in holders[:self.TOP_HOLDERS_PER_MARKET]:  # Top 25 per outcome
                    proxy_wallet = holder.get('proxyWallet')
                    if not proxy_wallet:
                        continue
                    
                    # Check if meets whale criteria
                    is_whale, user_data = self._check_whale_criteria_from_holder(holder, market_id)
                    
                    if is_whale:
                        whale_wallets.add(proxy_wallet)
                        
                        # Store user data (thread-safe)
                        if user_data:
                            with self._db_lock:
                                self.insert_or_replace('users', user_data)
                        
                        # Store holder record
                        holder_record = {
                            'market_id': market_id,
                            'token_id': token_id,
                            'proxy_wallet': proxy_wallet,
                            'username': holder.get('name'),
                            'pseudonym': holder.get('pseudonym'),
                            'amount': holder.get('amount', 0),
                            'outcome_index': outcome_index,
                            'bio': holder.get('bio'),
                            'profile_image': holder.get('profileImage'),
                            'updated_at': datetime.now().isoformat()
                        }
                        holder_records.append(holder_record)
            
            # Bulk insert holders (thread-safe)
            if holder_records:
                with self._db_lock:
                    self.bulk_insert_or_replace('market_holders', holder_records)
            
            return whale_wallets
            
        except Exception as e:
            self.logger.error(f"Error fetching holders for market {market_id}: {e}")
            return set()
    
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
    
    def _fetch_user_wallet_value(self, proxy_wallet: str) -> float:
        """
        Fetch user's total wallet value
        """
        try:
            url = f"{self.data_api_url}/portfolio-value"
            params = {"user": proxy_wallet}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('totalValue', 0) if data else 0
            
            return 0
            
        except Exception as e:
            return 0

    # ==================== PHASE 2: ENRICH WHALE USER DATA ====================
    
    def enrich_all_whale_users(self) -> Dict[str, int]:
        """
        Enrich all whale users with complete profile data using multithreading
        Returns statistics about enrichment process
        """
        self.logger.info("🔍 Enriching whale user profiles with multithreading...")
        
        # Get all whale users
        whales = self.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        whale_wallets = [w['proxy_wallet'] for w in whales]
        
        self.logger.info(f"Enriching {len(whale_wallets)} whale users using {self.max_workers} threads...")
        
        # Reset counters
        self._progress_counter = 0
        self._error_counter = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_wallet = {
                executor.submit(self._enrich_single_whale_thread_safe, wallet, len(whale_wallets)): wallet 
                for wallet in whale_wallets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_wallet):
                wallet = future_to_wallet[future]
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error enriching whale {wallet}: {e}")
        
        self.logger.info(f"✅ Enriched {self._progress_counter} whale users, Errors: {self._error_counter}")
        
        return {
            'total_whales_enriched': self._progress_counter,
            'errors': self._error_counter
        }
    
    def _enrich_single_whale_thread_safe(self, proxy_wallet: str, total_whales: int):
        """
        Thread-safe wrapper for enriching a single whale user
        """
        try:
            self._enrich_whale_user_data(proxy_wallet)
            
            with self._progress_lock:
                self._progress_counter += 1
                if self._progress_counter % 10 == 0:
                    self.logger.info(f"  Enriched {self._progress_counter}/{total_whales} whale users")
            
            # Rate limiting (distributed across threads)
            time.sleep(self.config.RATE_LIMIT_DELAY / self.max_workers)
            
        except Exception as e:
            with self._progress_lock:
                self._error_counter += 1
            raise e
    
    def _enrich_whale_user_data(self, proxy_wallet: str):
        """
        Fetch and store complete data for a whale user
        """
        # Use ThreadPoolExecutor for parallel sub-requests
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            
            # Fetch all user data in parallel (removed comments)
            futures.append(executor.submit(self._fetch_user_trades, proxy_wallet))
            futures.append(executor.submit(self._fetch_user_activity, proxy_wallet))
            futures.append(executor.submit(self._fetch_user_current_positions, proxy_wallet))
            futures.append(executor.submit(self._fetch_user_closed_positions, proxy_wallet))
            
            # Wait for all to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error in user enrichment subtask: {e}")
    
    def _fetch_user_trades(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user trade history"""
        try:
            url = f"{self.data_api_url}/trades"
            params = {"user": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                trades = response.json() or []
                if trades:
                    with self._db_lock:
                        self._store_user_trades(proxy_wallet, trades)
                return trades
            
            return []
            
        except Exception as e:
            return []
    
    def _fetch_user_activity(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user activity history"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {"user": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                activity = response.json() or []
                if activity:
                    with self._db_lock:
                        self._store_user_activity(proxy_wallet, activity)
                return activity
            
            return []
            
        except Exception as e:
            return []
    
    def _fetch_user_current_positions(self, proxy_wallet: str) -> List[Dict]:
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
                    with self._db_lock:
                        self._store_user_current_positions(proxy_wallet, positions)
                return positions
            
            return []
            
        except Exception as e:
            return []
    
    def _fetch_user_closed_positions(self, proxy_wallet: str) -> List[Dict]:
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
                    with self._db_lock:
                        self._store_user_closed_positions(proxy_wallet, positions)
                return positions
            
            return []
            
        except Exception as e:
            return []
    
    def _fetch_user_comments(self, proxy_wallet: str) -> List[Dict]:
        """Fetch user's comments"""
        try:
            url = f"{self.base_url}/comments"
            params = {"userAddress": proxy_wallet, "limit": 100}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                comments = response.json() or []
                
                if comments:
                    with self._db_lock:
                        self._store_user_comments(comments)
                    
                    # Fetch reactions for each comment (in parallel within this method)
                    self._fetch_comments_reactions_parallel(comments)
                
                return comments
            
            return []
            
        except Exception as e:
            return []
    
    def _fetch_comments_reactions_parallel(self, comments: List[Dict]):
        """Fetch reactions for multiple comments in parallel"""
        if not comments:
            return
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_comment_reactions, comment.get('id')): comment 
                for comment in comments if comment.get('id')
            }
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error fetching comment reactions: {e}")
    
    def _fetch_comment_reactions(self, comment_id: str) -> List[Dict]:
        """Fetch reactions for a specific comment"""
        try:
            url = f"{self.base_url}/comments/{comment_id}/reactions"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                reactions = response.json() or []
                
                if reactions:
                    with self._db_lock:
                        self._store_comment_reactions(comment_id, reactions)
                
                return reactions
            
            return []
            
        except Exception as e:
            return []
    
    # ==================== STORAGE METHODS ====================
    
    def _store_user_trades(self, proxy_wallet: str, trades: List[Dict]):
        """Store user trades (thread-safe when called with _db_lock)"""
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
                'username': trade.get('username'),
                'pseudonym': trade.get('pseudonym'),
                'bio': trade.get('bio'),
                'profile_image': trade.get('profileImage'),
                'profile_image_optimized': trade.get('profileImageOptimized')
            }
            trade_records.append(record)
        
        if trade_records:
            self.bulk_insert_or_replace('user_trades', trade_records)
    
    def _store_user_activity(self, proxy_wallet: str, activity: List[Dict]):
        """Store user activity (thread-safe when called with _db_lock)"""
        activity_records = []
        
        for act in activity:
            record = {
                'proxy_wallet': proxy_wallet,
                'timestamp': act.get('timestamp'),
                'condition_id': act.get('conditionId'),
                'transaction_hash': act.get('transactionHash'),
                'type': act.get('type'),
                'side': act.get('side'),
                'size': act.get('size'),
                'usdc_size': act.get('usdcSize'),
                'price': act.get('price'),
                'asset': act.get('asset'),
                'outcome_index': act.get('outcomeIndex'),
                'title': act.get('title'),
                'slug': act.get('slug'),
                'event_slug': act.get('eventSlug'),
                'outcome': act.get('outcome'),
                'username': act.get('username'),
                'pseudonym': act.get('pseudonym'),
                'bio': act.get('bio'),
                'profile_image': act.get('profileImage')
            }
            activity_records.append(record)
        
        if activity_records:
            self.bulk_insert_or_replace('user_activity', activity_records)
    
    def _store_user_current_positions(self, proxy_wallet: str, positions: List[Dict]):
        """Store user current positions (thread-safe when called with _db_lock)"""
        position_records = []
        
        for pos in positions:
            record = {
                'proxy_wallet': proxy_wallet,
                'asset': pos.get('asset'),
                'condition_id': pos.get('conditionId'),
                'size': pos.get('size'),
                'avg_price': pos.get('avgPrice'),
                'initial_value': pos.get('initialValue'),
                'current_value': pos.get('currentValue'),
                'cash_pnl': pos.get('cashPnl'),
                'percent_pnl': pos.get('percentPnl'),
                'total_bought': pos.get('totalBought'),
                'realized_pnl': pos.get('realizedPnl'),
                'percent_realized_pnl': pos.get('percentRealizedPnl'),
                'cur_price': pos.get('curPrice'),
                'redeemable': pos.get('redeemable', False),
                'mergeable': pos.get('mergeable', False),
                'negative_risk': pos.get('negativeRisk', False),
                'title': pos.get('title'),
                'slug': pos.get('slug'),
                'icon': pos.get('icon'),
                'event_id': pos.get('eventID'),
                'event_slug': pos.get('eventSlug'),
                'outcome': pos.get('outcome'),
                'outcome_index': pos.get('outcomeIndex'),
                'opposite_outcome': pos.get('oppositeOutcome'),
                'opposite_asset': pos.get('oppositeAsset'),
                'end_date': pos.get('endDate'),
                'updated_at': datetime.now().isoformat()
            }
            position_records.append(record)
        
        if position_records:
            self.bulk_insert_or_replace('user_positions_current', position_records)
    
    def _store_user_closed_positions(self, proxy_wallet: str, positions: List[Dict]):
        """Store user closed positions (thread-safe when called with _db_lock)"""
        position_records = []
        
        for pos in positions:
            record = {
                'proxy_wallet': proxy_wallet,
                'asset': pos.get('asset'),
                'condition_id': pos.get('conditionId'),
                'avg_price': pos.get('avgPrice'),
                'total_bought': pos.get('totalBought'),
                'realized_pnl': pos.get('realizedPnl'),
                'cur_price': pos.get('curPrice'),
                'title': pos.get('title'),
                'slug': pos.get('slug'),
                'icon': pos.get('icon'),
                'event_slug': pos.get('eventSlug'),
                'outcome': pos.get('outcome'),
                'outcome_index': pos.get('outcomeIndex'),
                'opposite_outcome': pos.get('oppositeOutcome'),
                'opposite_asset': pos.get('oppositeAsset'),
                'end_date': pos.get('endDate'),
                'closed_at': datetime.now().isoformat()
            }
            position_records.append(record)
        
        if position_records:
            self.bulk_insert_or_replace('user_positions_closed', position_records)
    
    def _store_user_comments(self, comments: List[Dict]):
        """Store user comments (thread-safe when called with _db_lock)"""
        comment_records = []
        
        for comment in comments:
            record = {
                'id': comment.get('id'),
                'event_id': comment.get('eventID'),
                'market_id': comment.get('marketID'),
                'proxy_wallet': comment.get('userAddress'),
                'username': comment.get('username'),
                'profile_image': comment.get('profileImage'),
                'content': comment.get('content'),
                'parent_comment_id': comment.get('parentCommentID'),
                'created_at': comment.get('createdAt'),
                'updated_at': comment.get('updatedAt'),
                'likes_count': comment.get('likesCount', 0),
                'replies_count': comment.get('repliesCount', 0)
            }
            comment_records.append(record)
        
        if comment_records:
            self.bulk_insert_or_replace('comments', comment_records)
    
    def _store_comment_reactions(self, comment_id: str, reactions: List[Dict]):
        """Store comment reactions (thread-safe when called with _db_lock)"""
        reaction_records = []
        
        for reaction in reactions:
            record = {
                'comment_id': comment_id,
                'proxy_wallet': reaction.get('userAddress'),
                'reaction_type': reaction.get('type', 'LIKE'),
                'created_at': reaction.get('createdAt') or datetime.now().isoformat()
            }
            reaction_records.append(record)
        
        if reaction_records:
            self.bulk_insert_or_replace('comment_reactions', reaction_records)
    
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