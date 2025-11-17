"""
ID events
Handles individual fetching for the events
"""

from threading import Lock
from typing import Dict, Optional

import requests
from ....database.database_manager import DatabaseManager
from ....config import Config

class IdEventsManager(DatabaseManager):
    """Manager for event-related operations with multithreading support"""
    
    def __init__(self):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        self._lock = Lock()  # Thread-safe database operations


    def fetch_event_by_id(self, event_id: str) -> Optional[Dict]:
            """
            Fetch detailed information for a specific event
            """
            try:
                url = f"{self.base_url}/events/{event_id}"
                params = {
                    "include_chat": "true",
                    "include_template": "true"
                }
                
                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                
                event = response.json()
                
                # Store the detailed event
                self._store_event_detailed(event)
                
                # Fetch and store tags
                self._fetch_and_store_event_tags(event_id, event.get('tags', []))
                
                # Fetch live volume if enabled
                if self.config.FETCH_LIVE_VOLUME:
                    self.fetch_event_live_volume(event_id)
                
                return event
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching event {event_id}: {e}")
                return None