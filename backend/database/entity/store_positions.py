from datetime import datetime
from threading import Lock
from typing import Dict, List
from backend.database.database_manager import DatabaseManager

class StorePositions(DatabaseManager):
    """Manager for storing positions with multithreading support"""

    def __init__(self, max_workers: int = None):
        super().__init__()
        from ...config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Set max workers (defaults to 20 for aggressive parallelization)
        self.max_workers = max_workers or min(10, (Config.MAX_WORKERS if hasattr(Config, 'MAX_WORKERS') else 10))
        
        # Thread-safe lock for database operations
        self._db_lock = Lock()
        
        # Thread-safe counters
        self._progress_lock = Lock()
        self._progress_counter = 0
        self._error_counter = 0
        self._comments_counter = 0
        self._reactions_counter = 0

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



    def _bulk_insert_closed_positions(self, positions: List[Dict]):
        """Bulk insert closed positions"""
        if not positions:
            return
        
        position_data = []
        
        for position in positions:
            position_data.append({
                'proxy_wallet': position.get('proxyWallet'),
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
                'end_date': position.get('endDate')
            })
        
        self.bulk_insert_or_ignore('user_positions_closed', position_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(position_data)} closed positions")




    def _bulk_insert_positions(self, positions: List[Dict]):
        """Bulk insert positions into database"""
        if not positions:
            return
        
        # Prepare data for bulk insert
        position_data = []
        
        for position in positions:
            position_data.append({
                'proxy_wallet': position.get('proxyWallet'),
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
                'end_date': position.get('endDate')
            })
        
        # Bulk insert
        self.bulk_insert_or_replace('user_positions_current', position_data, batch_size=100)
        self.logger.info(f"Bulk inserted {len(position_data)} positions")