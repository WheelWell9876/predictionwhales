"""
Batch events fetcher
Handles batch fetching operations for events from Polymarket API
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import requests
import logging

class BatchEventsFetcher:
    """Handles batch fetching of events with multithreading support"""
    
    def __init__(self, config, base_url: str):
        self.config = config
        self.base_url = base_url
        self.logger = logging.getLogger(self.__class__.__name__)

    def fetch_events_batch(self, offset: int, limit: int) -> List[Dict]:
        """
        Fetch a single batch of events
        
        Args:
            offset: Starting offset for pagination
            limit: Number of events to fetch
            
        Returns:
            List of event dictionaries
        """
        try:
            params = {
                "closed": "false",  # Only get active events
                "limit": limit,
                "offset": offset,
                "order": "volume",
                "ascending": "false"
            }
            
            response = requests.get(
                f"{self.base_url}/events",
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            events = response.json()
            return events if events else []
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching events at offset {offset}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error at offset {offset}: {e}")
            return []

    def fetch_all_events(self, limit: int = 100, num_threads: int = 5) -> List[Dict]:
        """
        Fetch all active events from the API with multithreading
        
        Args:
            limit: Events per request
            num_threads: Number of concurrent threads
            
        Returns:
            List of all fetched events
        """
        self.logger.info(f"Starting multithreaded fetch of active events ({num_threads} threads)...")
        
        # First batch to determine if we need more
        first_batch = self.fetch_events_batch(0, limit)
        if not first_batch:
            self.logger.warning("No events returned from API")
            return []
        
        all_events = first_batch.copy()
        
        # If we got fewer than limit, we're done
        if len(first_batch) < limit:
            self.logger.info(f"Total active events fetched: {len(all_events)}")
            return all_events
        
        # Generate offset list for parallel fetching
        self.logger.info(f"First batch: {len(first_batch)} events. Fetching remaining batches...")
        offsets = []
        offset = limit
        max_estimated_events = self.config.MAX_EVENTS_PER_RUN
        
        while offset < max_estimated_events:
            offsets.append(offset)
            offset += limit
        
        # Fetch batches concurrently
        completed_offsets = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all tasks
            future_to_offset = {
                executor.submit(self.fetch_events_batch, offset, limit): offset 
                for offset in offsets
            }
            
            # Process completed tasks
            for future in as_completed(future_to_offset):
                offset = future_to_offset[future]
                try:
                    events = future.result()
                    
                    if events:
                        all_events.extend(events)
                        completed_offsets.append(offset)
                        
                        if len(completed_offsets) % 10 == 0:
                            self.logger.info(f"Fetched {len(completed_offsets)} batches, total: {len(all_events)} events")
                        
                        # If we got fewer than limit, we've reached the end
                        if len(events) < limit:
                            self.logger.info(f"Reached end of events at offset {offset}")
                            # Cancel remaining futures
                            for f in future_to_offset.keys():
                                if not f.done():
                                    f.cancel()
                            break
                    else:
                        self.logger.info(f"Empty batch at offset {offset}, stopping")
                        break
                        
                except Exception as e:
                    self.logger.error(f"Error processing batch at offset {offset}: {e}")
                    continue
        
        self.logger.info(f"Total active events fetched: {len(all_events)}")
        return all_events