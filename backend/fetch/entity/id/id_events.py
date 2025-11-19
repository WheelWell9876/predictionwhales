"""
ID events fetcher
Handles individual event fetching operations from Polymarket API
"""

from typing import Dict, List, Optional
import requests
import logging

class IdEventsFetcher:
    """Handles individual event fetching operations"""
    
    def __init__(self, config, base_url: str, data_api_url: str):
        self.config = config
        self.base_url = base_url
        self.data_api_url = data_api_url
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_event_by_id(self, event_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific event
        
        Args:
            event_id: The event ID to fetch
            
        Returns:
            Event dictionary or None if failed
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
            self.logger.debug(f"Successfully fetched event: {event_id}")
            return event
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching event {event_id}: {e}")
            return None

    def fetch_event_live_volume(self, event_id: str) -> Optional[Dict]:
        """
        Fetch live volume data for a specific event
        
        Args:
            event_id: The event ID
            
        Returns:
            Volume data dictionary or None if failed
        """
        if not self.config.FETCH_LIVE_VOLUME:
            return None
            
        try:
            url = f"{self.data_api_url}/events/{event_id}/volume"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            volume_data = response.json()
            self.logger.debug(f"Successfully fetched live volume for event: {event_id}")
            return volume_data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching live volume for event {event_id}: {e}")
            return None

    def fetch_event_tags(self, event_id: str) -> List[Dict]:
        """
        Fetch tags for a specific event
        
        Args:
            event_id: The event ID
            
        Returns:
            List of tag dictionaries
        """
        if not self.config.FETCH_TAGS:
            return []
            
        try:
            url = f"{self.base_url}/events/{event_id}/tags"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            self.logger.debug(f"Successfully fetched {len(tags)} tags for event: {event_id}")
            return tags if tags else []
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for event {event_id}: {e}")
            return []