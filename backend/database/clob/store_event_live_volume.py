from datetime import datetime
import json
from typing import Dict


def _store_live_volume(self, event_id: str, volume_data: Dict):
    """
    Store live volume data for an event
    """
    record = {
        'event_id': event_id,
        'total_volume': volume_data.get('total', 0),
        'market_volumes': json.dumps(volume_data.get('markets', [])),
        'timestamp': datetime.now().isoformat()
    }
    
    self.insert_or_replace('event_live_volume', record)
    
    # Update event volume
    self.update_record(
        'events',
        {'volume': volume_data.get('total', 0), 'updated_at': datetime.now().isoformat()},
        'id = ?',
        (event_id,)
    )
    
    self.logger.debug(f"Stored live volume for event {event_id}: ${volume_data.get('total', 0):,.2f}")