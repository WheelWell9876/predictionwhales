"""
Events Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing event data with concurrent requests
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from .database_manager import DatabaseManager
from .config import Config

class EventsManager(DatabaseManager):
    """Manager for event-related operations with multithreading support"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
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
    
    def fetch_event_tags(self, event_id: str) -> List[Dict]:
        """
        Fetch tags for a specific event
        """
        try:
            url = f"{self.base_url}/events/{event_id}/tags"
            
            response = requests.get(
                url,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            
            # Store tags and relationships
            self._fetch_and_store_event_tags(event_id, tags)
            
            return tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags for event {event_id}: {e}")
            return []
    
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
    
    def _store_events(self, events: List[Dict]):
        """
        Store multiple events in the database (thread-safe)
        """
        event_records = []
        
        for event in events:
            record = {
                'id': event.get('id'),
                'ticker': event.get('ticker'),
                'slug': event.get('slug'),
                'title': event.get('title'),
                'description': event.get('description'),
                'start_date': event.get('startDate'),
                'creation_date': event.get('creationDate'),
                'end_date': event.get('endDate'),
                'image': event.get('image'),
                'icon': event.get('icon'),
                'liquidity': event.get('liquidity'),
                'liquidity_clob': event.get('liquidityClob'),
                'volume': event.get('volume'),
                'volume_clob': event.get('volumeClob'),
                'volume_24hr': event.get('volume24hr'),
                'volume_24hr_clob': event.get('volume24hrClob'),
                'volume_1wk': event.get('volume1wk'),
                'volume_1wk_clob': event.get('volume1wkClob'),
                'volume_1mo': event.get('volume1mo'),
                'volume_1mo_clob': event.get('volume1moClob'),
                'volume_1yr': event.get('volume1yr'),
                'volume_1yr_clob': event.get('volume1yrClob'),
                'open_interest': event.get('openInterest'),
                'competitive': event.get('competitive'),
                'comment_count': event.get('commentCount'),
                'active': event.get('active'),
                'closed': event.get('closed'),
                'archived': event.get('archived'),
                'new': event.get('new'),
                'featured': event.get('featured'),
                'restricted': event.get('restricted'),
                'enable_order_book': event.get('enableOrderBook'),
                'cyom': event.get('cyom'),
                'show_all_outcomes': event.get('showAllOutcomes'),
                'show_market_images': event.get('showMarketImages'),
                'enable_neg_risk': event.get('enableNegRisk'),
                'automatically_active': event.get('automaticallyActive'),
                'neg_risk_augmented': event.get('negRiskAugmented'),
                'pending_deployment': event.get('pendingDeployment'),
                'deploying': event.get('deploying'),
                'created_at': event.get('createdAt'),
                'updated_at': event.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            event_records.append(record)
            
            # Store basic tags if present
            if 'tags' in event:
                self._store_event_tags_basic(event['id'], event['tags'])
        
        # Bulk insert events
        if event_records:
            self.bulk_insert_or_replace('events', event_records)
    
    def _store_event_detailed(self, event: Dict):
        """
        Store detailed event information
        """
        record = {
            'id': event.get('id'),
            'ticker': event.get('ticker'),
            'slug': event.get('slug'),
            'title': event.get('title'),
            'description': event.get('description'),
            'start_date': event.get('startDate'),
            'creation_date': event.get('creationDate'),
            'end_date': event.get('endDate'),
            'image': event.get('image'),
            'icon': event.get('icon'),
            'liquidity': event.get('liquidity'),
            'liquidity_clob': event.get('liquidityClob'),
            'volume': event.get('volume'),
            'volume_clob': event.get('volumeClob'),
            'volume_24hr': event.get('volume24hr'),
            'volume_24hr_clob': event.get('volume24hrClob'),
            'volume_1wk': event.get('volume1wk'),
            'volume_1wk_clob': event.get('volume1wkClob'),
            'volume_1mo': event.get('volume1mo'),
            'volume_1mo_clob': event.get('volume1moClob'),
            'volume_1yr': event.get('volume1yr'),
            'volume_1yr_clob': event.get('volume1yrClob'),
            'open_interest': event.get('openInterest'),
            'competitive': event.get('competitive'),
            'comment_count': event.get('commentCount'),
            'active': event.get('active'),
            'closed': event.get('closed'),
            'archived': event.get('archived'),
            'new': event.get('new'),
            'featured': event.get('featured'),
            'restricted': event.get('restricted'),
            'enable_order_book': event.get('enableOrderBook'),
            'cyom': event.get('cyom'),
            'show_all_outcomes': event.get('showAllOutcomes'),
            'show_market_images': event.get('showMarketImages'),
            'enable_neg_risk': event.get('enableNegRisk'),
            'automatically_active': event.get('automaticallyActive'),
            'neg_risk_augmented': event.get('negRiskAugmented'),
            'pending_deployment': event.get('pendingDeployment'),
            'deploying': event.get('deploying'),
            'created_at': event.get('createdAt'),
            'updated_at': event.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }
        
        self.insert_or_replace('events', record)
        self.logger.debug(f"Stored detailed event: {event.get('id')}")
    
    def _store_event_tags_basic(self, event_id: str, tags: List[Dict]):
        """
        Store basic tag information from event response
        """
        for tag in tags:
            tag_id = tag.get('id')
            
            # Store tag
            tag_record = {
                'id': tag_id,
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            }
            self.insert_or_ignore('tags', tag_record)
            
            # Store event-tag relationship
            conn = self.get_persistent_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT event_ids FROM event_tags WHERE tag_id = ?", (tag_id,))
            result = cursor.fetchone()
            
            if result:
                event_ids = json.loads(result[0])
                if event_id not in event_ids:
                    event_ids.append(event_id)
                    cursor.execute(
                        "UPDATE event_tags SET event_ids = ? WHERE tag_id = ?",
                        (json.dumps(event_ids), tag_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO event_tags (tag_id, event_ids) VALUES (?, ?)",
                    (tag_id, json.dumps([event_id]))
                )
            
            conn.commit()
    
    def _fetch_and_store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store detailed tag information and relationships
        """
        tag_records = []
        
        for tag in tags:
            tag_record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            tag_records.append(tag_record)
        
        # Bulk insert tags
        if tag_records:
            self.bulk_insert_or_replace('tags', tag_records)
        
        # Store event-tag relationships
        for tag in tags:
            tag_id = tag.get('id')
            
            conn = self.get_persistent_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT event_ids FROM event_tags WHERE tag_id = ?", (tag_id,))
            result = cursor.fetchone()
            
            if result:
                event_ids = json.loads(result[0])
                if event_id not in event_ids:
                    event_ids.append(event_id)
                    cursor.execute(
                        "UPDATE event_tags SET event_ids = ? WHERE tag_id = ?",
                        (json.dumps(event_ids), tag_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO event_tags (tag_id, event_ids) VALUES (?, ?)",
                    (tag_id, json.dumps([event_id]))
                )
            
            conn.commit()
        
        self.logger.debug(f"Stored {len(tags)} tags for event {event_id}")
    
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
    
    def process_all_events_detailed(self, num_threads: int = 10):
        """
        Process all events to fetch detailed information with multithreading
        
        Args:
            num_threads: Number of concurrent threads (default: 10)
        """
        # Get all event IDs from database
        events = self.fetch_all("SELECT id, slug FROM events ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(events)} events for detailed information ({num_threads} threads)...")
        
        processed = 0
        errors = 0
        lock = Lock()
        
        def process_event(event):
            nonlocal processed, errors
            try:
                self.fetch_event_by_id(event['id'])
                with lock:
                    processed += 1
                    if processed % 50 == 0:
                        self.logger.info(f"Processed {processed}/{len(events)} events")
            except Exception as e:
                with lock:
                    errors += 1
                self.logger.error(f"Error processing event {event['id']}: {e}")
        
        # Process events concurrently
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            executor.map(process_event, events)
        
        self.logger.info(f"✅ Event processing complete. Processed: {processed}, Errors: {errors}")
    
    def daily_scan(self):
        """
        Perform daily scan for new events
        ONLY fetches active events
        """
        self.logger.info("Starting daily event scan...")
        
        # Fetch ONLY active events with multithreading
        active_events = self.fetch_all_events(closed=False, num_threads=5)
        
        # Process detailed information for new events with multithreading
        self.process_all_events_detailed(num_threads=10)
        
        self.logger.info("Daily event scan complete")
        
        return len(active_events)
    
    def remove_closed_events(self):
        """
        Remove all closed events from the database
        Use this to clean up if closed events were accidentally fetched
        """
        self.logger.info("Removing closed events from database...")
        
        # Count closed events
        result = self.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
        closed_count = result['count'] if result else 0
        
        self.logger.info(f"Found {closed_count} closed events to remove")
        
        if closed_count > 0:
            # Delete closed events
            deleted = self.delete_records('events', 'closed = 1', commit=True)
            self.logger.info(f"Removed {deleted} closed events")
            
            # Count remaining
            result = self.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 0")
            remaining = result['count'] if result else 0
            self.logger.info(f"Remaining active events: {remaining}")
            
            return deleted
        else:
            self.logger.info("No closed events to remove")
            return 0