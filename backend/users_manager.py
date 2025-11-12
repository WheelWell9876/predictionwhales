"""
Users Manager for Polymarket Terminal - WHALE FOCUSED
Handles fetching high-value users (whales) and their complete activity profiles
Focused on users with $1000+ wallets or $250+ positions
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from .database_manager import DatabaseManager

class UsersManager(DatabaseManager):
    """Manager for whale user operations"""

    def __init__(self):
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

    # ==================== PHASE 1: FETCH TOP HOLDERS FROM MARKETS ====================
    
    def fetch_top_holders_for_all_markets(self) -> Dict[str, int]:
        """
        Fetch top 25 holders for ALL active markets
        Only store users meeting whale criteria ($1000+ wallet OR $250+ position)
        Returns: {'total_markets_processed': X, 'total_whales_found': Y}
        """
        self.logger.info("🐋 Fetching top holders for all active markets...")
        
        # Get all active markets
        markets = self.fetch_all("""
            SELECT id, condition_id, question
            FROM markets
            WHERE active = 1 AND condition_id IS NOT NULL
            ORDER BY volume DESC
        """)
        
        total_whales = set()
        markets_processed = 0
        
        for market in markets:
            try:
                whales_in_market = self._fetch_and_filter_market_holders(
                    market['id'], 
                    market['condition_id']
                )
                total_whales.update(whales_in_market)
                markets_processed += 1
                
                if markets_processed % 10 == 0:
                    self.logger.info(f"  Processed {markets_processed}/{len(markets)} markets, found {len(total_whales)} unique whales")
                
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing market {market['id']}: {e}")
                continue
        
        self.logger.info(f"✅ Found {len(total_whales)} whale users across {markets_processed} markets")
        
        return {
            'total_markets_processed': markets_processed,
            'total_whales_found': len(total_whales)
        }
    
    def _fetch_and_filter_market_holders(self, market_id: str, condition_id: str) -> Set[str]:
        """
        Fetch top 25 holders for a specific market and filter by whale criteria
        Returns set of wallet addresses that meet whale criteria
        """
        try:
            # Fetch market holders
            url = f"{self.clob_url}/markets/{condition_id}/participants"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200:
                return set()
            
            data = response.json()
            
            if not data or 'data' not in data:
                return set()
            
            participants = data['data'][:self.TOP_HOLDERS_PER_MARKET]  # Top 25
            
            whale_wallets = set()
            holder_records = []
            
            for participant in participants:
                proxy_wallet = participant.get('address')
                if not proxy_wallet:
                    continue
                
                # Check if meets whale criteria
                is_whale, user_data = self._check_whale_criteria(participant, market_id)
                
                if is_whale:
                    whale_wallets.add(proxy_wallet)
                    
                    # Store user data
                    if user_data:
                        self.insert_or_replace('users', user_data)
                    
                    # Store holder record
                    holder_record = {
                        'market_id': market_id,
                        'token_id': participant.get('tokenID'),
                        'proxy_wallet': proxy_wallet,
                        'username': participant.get('username'),
                        'pseudonym': participant.get('pseudonym'),
                        'amount': participant.get('shares', 0),
                        'outcome_index': participant.get('outcomeIndex'),
                        'bio': participant.get('bio'),
                        'profile_image': participant.get('profileImage'),
                        'updated_at': datetime.now().isoformat()
                    }
                    holder_records.append(holder_record)
            
            # Bulk insert holders
            if holder_records:
                self.bulk_insert_or_replace('market_holders', holder_records)
            
            return whale_wallets
            
        except Exception as e:
            self.logger.error(f"Error fetching holders for market {market_id}: {e}")
            return set()
    
    def _check_whale_criteria(self, participant: Dict, market_id: str) -> tuple[bool, Optional[Dict]]:
        """
        Check if participant meets whale criteria:
        - $1000+ wallet value OR
        - $250+ position value
        
        Returns: (is_whale: bool, user_data: Dict or None)
        """
        proxy_wallet = participant.get('address')
        
        # First check: Get wallet value
        wallet_value = self._fetch_user_wallet_value(proxy_wallet)
        
        # Second check: Check position value
        position_value = participant.get('shares', 0) * participant.get('price', 0)
        
        is_whale = wallet_value >= self.MIN_WALLET_VALUE or position_value >= self.MIN_POSITION_VALUE
        
        if not is_whale:
            return False, None
        
        # Prepare user record
        user_data = {
            'proxy_wallet': proxy_wallet,
            'username': participant.get('username'),
            'pseudonym': participant.get('pseudonym'),
            'bio': participant.get('bio'),
            'profile_image': participant.get('profileImage'),
            'profile_image_optimized': participant.get('profileImageOptimized'),
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
    
    # ==================== PHASE 2: FETCH WHALE USER DETAILS ====================
    
    def fetch_whale_user_complete_profile(self, proxy_wallet: str) -> Dict:
        """
        Fetch COMPLETE profile for a whale user:
        - Top 10 trades by size
        - Top 10 user activity by size
        - Wallet value
        - Top 10 largest positions
        - Top 10 closed positions
        - Top 10 current positions
        - Comments and reactions
        
        Returns summary dict
        """
        self.logger.info(f"Fetching complete profile for whale: {proxy_wallet[:10]}...")
        
        summary = {
            'wallet': proxy_wallet,
            'trades_fetched': 0,
            'activity_fetched': 0,
            'current_positions': 0,
            'closed_positions': 0,
            'wallet_value': 0,
            'comments_fetched': 0,
            'reactions_fetched': 0
        }
        
        try:
            # 1. Wallet value
            summary['wallet_value'] = self.fetch_user_portfolio_value(proxy_wallet)
            
            # 2. Top 10 trades by size
            trades = self.fetch_user_top_trades(proxy_wallet, limit=10)
            summary['trades_fetched'] = len(trades)
            
            # 3. Top 10 activity by size
            activity = self.fetch_user_top_activity(proxy_wallet, limit=10)
            summary['activity_fetched'] = len(activity)
            
            # 4. Top 10 current positions
            current_pos = self.fetch_user_top_current_positions(proxy_wallet, limit=10)
            summary['current_positions'] = len(current_pos)
            
            # 5. Top 10 closed positions
            closed_pos = self.fetch_user_top_closed_positions(proxy_wallet, limit=10)
            summary['closed_positions'] = len(closed_pos)
            
            # 6. Comments and reactions
            comments, reactions = self.fetch_user_comments_and_reactions(proxy_wallet)
            summary['comments_fetched'] = len(comments)
            summary['reactions_fetched'] = len(reactions)
            
            # Update user record with latest data
            self.update_record(
                'users',
                {
                    'total_value': summary['wallet_value'],
                    'last_updated': datetime.now().isoformat()
                },
                'proxy_wallet = ?',
                (proxy_wallet,)
            )
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error fetching profile for {proxy_wallet}: {e}")
            return summary
    
    def fetch_all_whale_profiles(self):
        """
        Fetch complete profiles for ALL whales in database
        """
        self.logger.info("🐋 Fetching complete profiles for all whales...")
        
        # Get all whale wallets
        whales = self.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        
        total_whales = len(whales)
        processed = 0
        
        for whale in whales:
            try:
                self.fetch_whale_user_complete_profile(whale['proxy_wallet'])
                processed += 1
                
                if processed % 10 == 0:
                    self.logger.info(f"  Processed {processed}/{total_whales} whale profiles")
                
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing whale {whale['proxy_wallet']}: {e}")
                continue
        
        self.logger.info(f"✅ Processed {processed} whale profiles")
    
    # ==================== USER DATA FETCHING METHODS ====================
    
    def fetch_user_portfolio_value(self, proxy_wallet: str) -> float:
        """Fetch user's portfolio value and store in user_values table"""
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
                total_value = data.get('totalValue', 0) if data else 0
                
                # Store value record
                value_record = {
                    'proxy_wallet': proxy_wallet,
                    'market_condition_id': None,  # NULL for total portfolio
                    'value': total_value,
                    'timestamp': datetime.now().isoformat()
                }
                self.insert_or_replace('user_values', value_record)
                
                return total_value
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error fetching portfolio value for {proxy_wallet}: {e}")
            return 0
    
    def fetch_user_top_trades(self, proxy_wallet: str, limit: int = 10) -> List[Dict]:
        """Fetch top trades by size"""
        try:
            url = f"{self.data_api_url}/trades"
            params = {
                "user": proxy_wallet,
                "limit": 100,  # Fetch more, then sort
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
                    # Sort by size and take top N
                    sorted_trades = sorted(trades, key=lambda x: float(x.get('size', 0)), reverse=True)[:limit]
                    self._store_user_trades(proxy_wallet, sorted_trades)
                    return sorted_trades
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching trades for {proxy_wallet}: {e}")
            return []
    
    def fetch_user_top_activity(self, proxy_wallet: str, limit: int = 10) -> List[Dict]:
        """Fetch top activity by size"""
        try:
            url = f"{self.data_api_url}/activity"
            params = {
                "user": proxy_wallet,
                "limit": 100,
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
                activity = response.json()
                
                if activity:
                    # Sort by USDC size and take top N
                    sorted_activity = sorted(activity, key=lambda x: float(x.get('usdcSize', 0)), reverse=True)[:limit]
                    self._store_user_activity(proxy_wallet, sorted_activity)
                    return sorted_activity
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching activity for {proxy_wallet}: {e}")
            return []
    
    def fetch_user_top_current_positions(self, proxy_wallet: str, limit: int = 10) -> List[Dict]:
        """Fetch top 10 current positions by value"""
        try:
            url = f"{self.data_api_url}/positions"
            params = {
                "user": proxy_wallet,
                "sizeThreshold": "1",
                "limit": 100,
                "sortBy": "VALUE",  # Sort by value
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
                    top_positions = positions[:limit]
                    self._store_user_current_positions(proxy_wallet, top_positions)
                    return top_positions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching current positions for {proxy_wallet}: {e}")
            return []
    
    def fetch_user_top_closed_positions(self, proxy_wallet: str, limit: int = 10) -> List[Dict]:
        """Fetch top 10 closed positions"""
        try:
            url = f"{self.data_api_url}/closed-positions"
            params = {
                "user": proxy_wallet,
                "limit": limit
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
                    self._store_user_closed_positions(proxy_wallet, positions)
                    return positions
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error fetching closed positions for {proxy_wallet}: {e}")
            return []
    
    def fetch_user_comments_and_reactions(self, proxy_wallet: str) -> tuple[List[Dict], List[Dict]]:
        """
        Fetch all comments and reactions for a user
        Returns: (comments, reactions)
        """
        comments = []
        reactions = []
        
        try:
            # Fetch user's comments
            url = f"{self.base_url}/comments"
            params = {
                "user": proxy_wallet,
                "limit": 100
            }
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                comments = response.json() or []
                
                # Store comments
                if comments:
                    self._store_user_comments(comments)
                    
                    # For each comment, fetch its reactions
                    for comment in comments:
                        comment_reactions = self._fetch_comment_reactions(comment['id'])
                        reactions.extend(comment_reactions)
            
            return comments, reactions
            
        except Exception as e:
            self.logger.error(f"Error fetching comments for {proxy_wallet}: {e}")
            return comments, reactions
    
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
                    self._store_comment_reactions(comment_id, reactions)
                
                return reactions
            
            return []
            
        except Exception as e:
            return []
    
    # ==================== STORAGE METHODS ====================
    
    def _store_user_trades(self, proxy_wallet: str, trades: List[Dict]):
        """Store user trades"""
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
        """Store user activity"""
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
        """Store user current positions"""
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
        """Store user closed positions"""
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
        """Store user comments"""
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
        """Store comment reactions"""
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
    
    # ==================== LEGACY/COMPATIBILITY METHODS ====================
    
    def fetch_top_holders_for_markets(self, limit_markets: int = 100) -> int:
        """Legacy method - calls new whale-focused method"""
        result = self.fetch_top_holders_for_all_markets()
        return result['total_whales_found']
    
    def identify_whale_users(self) -> List[str]:
        """Get list of all whale wallet addresses"""
        whales = self.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        return [w['proxy_wallet'] for w in whales]