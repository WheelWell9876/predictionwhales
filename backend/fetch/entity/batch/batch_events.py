"""
Batch events
Handles batch fetching for the events
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Dict, List

import requests
from ....database.database_manager import DatabaseManager
from ....config import Config

class BatchEventsManager(DatabaseManager):
    """Manager for event-related operations with multithreading support"""
    
    def __init__(self):
        super().__init__()
        from ....config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        self._lock = Lock()  # Thread-safe database operations


    def fetch_all_events(self, closed: bool = False, limit: int = 100, num_threads: int = 5) -> List[Dict]:
            """
            Fetch all events from the API with multithreading
            ONLY fetches active events (closed=false)
            
            Args:
                closed: Ignored, always fetches active events
                limit: Events per request
                num_threads: Number of concurrent threads (default: 5)
            """
            self.logger.info(f"Starting MULTITHREADED fetch of active events ({num_threads} threads)...")
            
            # First, get total count to calculate offsets
            first_batch = self._fetch_events_batch(0, limit)
            if not first_batch:
                self.logger.warning("No events returned from API")
                return []
            
            # Store first batch
            self._store_events(first_batch)
            all_events = first_batch.copy()
            
            # If we got fewer than limit, we're done
            if len(first_batch) < limit:
                self.logger.info(f"Total active events fetched: {len(all_events)}")
                return all_events
            
            # Calculate how many more batches we need
            # We'll fetch until we get a batch with < limit events
            self.logger.info(f"First batch: {len(first_batch)} events. Fetching remaining batches...")
            
            # Generate offset list (start from next batch)
            offsets = []
            offset = limit
            # Estimate max offsets (we'll break when we get empty results)
            max_estimated_events = 10000  # Adjust based on your needs
            while offset < max_estimated_events:
                offsets.append(offset)
                offset += limit
            
            # Fetch batches concurrently
            completed_offsets = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Submit all tasks
                future_to_offset = {
                    executor.submit(self._fetch_events_batch, offset, limit): offset 
                    for offset in offsets
                }
                
                # Process completed tasks
                for future in as_completed(future_to_offset):
                    offset = future_to_offset[future]
                    try:
                        events = future.result()
                        
                        if events:
                            # Thread-safe storage
                            with self._lock:
                                self._store_events(events)
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
                            # Empty result, we've passed the end
                            self.logger.info(f"Empty batch at offset {offset}, stopping")
                            break
                            
                    except Exception as e:
                        self.logger.error(f"Error fetching batch at offset {offset}: {e}")
                        continue
            
            self.logger.info(f"✅ Total active events fetched: {len(all_events)} (using {num_threads} threads)")
            return all_events
    
    def _fetch_events_batch(self, offset: int, limit: int) -> List[Dict]:
        """
        Fetch a single batch of events (thread-safe)
        """
        try:
            params = {
                "closed": "false",  # ALWAYS false - only get active events
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