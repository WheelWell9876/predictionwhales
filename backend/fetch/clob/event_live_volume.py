


from typing import Dict, Optional

import requests


def fetch_event_live_volume(self, event_id: str) -> Optional[Dict]:
        """
        Fetch live volume data for an event
        """
        if not self.config.FETCH_LIVE_VOLUME:
            return None
            
        try:
            url = f"{self.data_api_url}/live-volume"
            params = {"id": event_id}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                volume_data = data[0]
                self._store_live_volume(event_id, volume_data)
                return volume_data
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching live volume for event {event_id}: {e}")
            return None