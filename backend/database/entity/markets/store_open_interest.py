import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database.database_manager import DatabaseManager
from .config import Config



def _store_open_interest(self, market_id: str, condition_id: str, oi_value: float):
    """
    Store open interest data for a market
    Thread-safe when called with _db_lock
    """
    record = {
        'market_id': market_id,
        'condition_id': condition_id,
        'open_interest': oi_value,
        'timestamp': datetime.now().isoformat()
    }
    
    self.insert_or_replace('market_open_interest', record)
    
    # Update market with open interest
    self.update_record(
        'markets',
        {'open_interest': oi_value, 'updated_at': datetime.now().isoformat()},
        'id = ?',
        (market_id,)
    )
    
    self.logger.debug(f"Stored open interest for market {market_id}: ${oi_value:,.2f}")