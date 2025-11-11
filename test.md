The first issue I want to fix is my event tags table. This is a table that is being written very inefficiently. This function goes through every event that is queried, finds a tag that it is linked to, and it writes a new object with the event and tag per object. The thing is, events have many tags, and tags are linked to many events, so I am creating tons of duplicates for each tag and event that is being found in the events. To fix this table of event_tags, I want to have each tag be an object in the database, and then for that tag, I want there to be a list of events that the tag appears in. In doing this, I will not need to write a new object for each event and tag pair. What I mean by this is that I want to have the tags to be the primary key for this table, and I want the events that include this tag should all be in this row/ they should all be as ONE OBJECT for this tag. 

Here is the current code for those files in which I will need to modify to make these tables:

"""
Tags Manager for Polymarket Terminal
Handles fetching, processing, and storing tag data and relationships
"""

import requests
import time
from datetime import datetime
from typing import Dict, List, Optional
from .database_manager import DatabaseManager
from .config import Config

class TagsManager(DatabaseManager):
    """Manager for tag-related operations"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
    
    def fetch_all_tags(self, limit: int = 1000) -> List[Dict]:
        """
        Fetch all tags from the API
        Used for initial data load and daily scans
        """
        all_tags = []
        
        self.logger.info("Starting to fetch all tags...")
        
        try:
            url = f"{self.base_url}/tags"
            params = {"limit": limit}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tags = response.json()
            all_tags.extend(tags)
            
            # Store tags
            self._store_tags(tags)
            
            self.logger.info(f"Fetched and stored {len(tags)} tags")
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tags: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        
        return all_tags
    
    def fetch_tag_by_id(self, tag_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}"
            params = {"include_template": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            tag = response.json()
            
            # Store detailed tag
            self._store_tag_detailed(tag)
            
            return tag
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching tag {tag_id}: {e}")
            return None
    
    def fetch_tag_relationships(self, tag_id: str) -> List[Dict]:
        """
        Fetch relationships for a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}/related-tags"
            params = {"status": "all", "omit_empty": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            relationships = response.json()
            
            if relationships:
                self._store_tag_relationships(relationships)
            
            return relationships
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching relationships for tag {tag_id}: {e}")
            return []
    
    def fetch_related_tags_details(self, tag_id: str) -> List[Dict]:
        """
        Fetch full details of tags related to a specific tag
        """
        try:
            url = f"{self.base_url}/tags/{tag_id}/related-tags/tags"
            params = {"status": "all", "omit_empty": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            related_tags = response.json()
            
            if related_tags:
                self._store_tags(related_tags, detailed=True)
            
            return related_tags
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching related tags details for tag {tag_id}: {e}")
            return []
    
    def _store_tags(self, tags: List[Dict], detailed: bool = False):
        """
        Store multiple tags in the database
        """
        tag_records = []
        
        for tag in tags:
            record = self._prepare_tag_record(tag, detailed)
            tag_records.append(record)
        
        # Bulk insert tags
        if tag_records:
            self.bulk_insert_or_replace('tags', tag_records)
            self.logger.debug(f"Stored {len(tag_records)} tags")
    
    def _store_tag_detailed(self, tag: Dict):
        """
        Store detailed tag information
        """
        record = self._prepare_tag_record(tag, detailed=True)
        self.insert_or_replace('tags', record)
        self.logger.debug(f"Stored detailed tag: {tag.get('id')}")
    
    def _prepare_tag_record(self, tag: Dict, detailed: bool = False) -> Dict:
        """
        Prepare a tag record for database insertion
        """
        record = {
            'id': tag.get('id'),
            'label': tag.get('label'),
            'slug': tag.get('slug'),
            'force_show': tag.get('forceShow', False),
            'fetched_at': datetime.now().isoformat()
        }
        
        if detailed:
            record.update({
                'force_hide': tag.get('forceHide', False),
                'is_carousel': tag.get('isCarousel', False),
                'published_at': tag.get('publishedAt'),
                'created_by': tag.get('createdBy'),
                'updated_by': tag.get('updatedBy'),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            })
        
        return record
    
    def _store_tag_relationships(self, relationships: List[Dict]):
        """
        Store tag relationships in the database
        """
        relationship_records = []
        
        for rel in relationships:
            record = {
                'id': rel.get('id'),
                'tag_id': rel.get('tagID'),
                'related_tag_id': rel.get('relatedTagID'),
                'rank': rel.get('rank'),
                'created_at': datetime.now().isoformat()
            }
            relationship_records.append(record)
        
        # Bulk insert relationships
        if relationship_records:
            self.bulk_insert_or_replace('tag_relationships', relationship_records)
            self.logger.debug(f"Stored {len(relationship_records)} tag relationships")
    
    def process_all_tags_detailed(self):
        """
        Process all tags to fetch detailed information and relationships
        """
        # Get all tag IDs from database
        tags = self.fetch_all("SELECT id FROM tags ORDER BY id")
        
        self.logger.info(f"Processing {len(tags)} tags for detailed information...")
        
        processed = 0
        errors = 0
        total_relationships = 0
        
        for tag in tags:
            try:
                tag_id = tag['id']
                
                # Fetch detailed tag information
                self.fetch_tag_by_id(tag_id)
                
                # Fetch tag relationships
                relationships = self.fetch_tag_relationships(tag_id)
                total_relationships += len(relationships)
                
                # Fetch related tags details
                self.fetch_related_tags_details(tag_id)
                
                processed += 1
                
                if processed % 50 == 0:
                    self.logger.info(f"Processed {processed}/{len(tags)} tags")
                    self.logger.info(f"Total relationships found: {total_relationships}")
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing tag {tag['id']}: {e}")
                errors += 1
        
        self.logger.info(f"Tag processing complete. Processed: {processed}, Errors: {errors}")
        self.logger.info(f"Total relationships stored: {total_relationships}")
    
    def daily_scan(self):
        """
        Perform daily scan for tag updates
        """
        if not self.config.FETCH_TAGS:
            self.logger.info("Tag fetching disabled")
            return 0
        
        self.logger.info("Starting daily tag scan...")
        
        # Fetch all tags
        all_tags = self.fetch_all_tags()
        
        # Process detailed information and relationships
        self.process_all_tags_detailed()
        
        # Get statistics
        stats = self._get_tag_statistics()
        
        self.logger.info(f"Daily tag scan complete. Total tags: {len(all_tags)}")
        self.logger.info(f"Tag statistics: {stats}")
        
        return len(all_tags)
    
    def _get_tag_statistics(self) -> Dict:
        """
        Get statistics about tags in the database
        """
        stats = {}
        
        # Total tags
        stats['total_tags'] = self.get_table_count('tags')
        
        # Tags with relationships
        result = self.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count
            FROM tag_relationships
        """)
        stats['tags_with_relationships'] = result['count'] if result else 0
        
        # Total relationships
        stats['total_relationships'] = self.get_table_count('tag_relationships')
        
        # Most used tags (in events)
        result = self.fetch_all("""
            SELECT t.label, COUNT(et.event_id) as usage_count
            FROM tags t
            JOIN event_tags et ON t.id = et.tag_id
            GROUP BY t.id, t.label
            ORDER BY usage_count DESC
            LIMIT 10
        """)
        stats['most_used_tags'] = result
        
        return stats



"""
Events Manager for Polymarket Terminal
Handles fetching, processing, and storing event data
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from .database_manager import DatabaseManager
from .config import Config

class EventsManager(DatabaseManager):
    """Manager for event-related operations"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        self.data_api_url = Config.DATA_API_URL
        
    def fetch_all_events(self, closed: bool = False, limit: int = 100) -> List[Dict]:
        """
        Fetch all events from the API
        Used for initial data load and daily scans
        """
        all_events = []
        offset = 0
        
        self.logger.info(f"Starting to fetch all {'closed' if closed else 'active'} events...")
        
        while True:
            try:
                # Prepare request parameters
                params = {
                    "closed": str(closed).lower(),
                    "limit": limit,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "false"
                }
                
                # Make API request
                response = requests.get(
                    f"{self.base_url}/events",
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                
                events = response.json()
                
                if not events:
                    break
                
                all_events.extend(events)
                self.logger.info(f"Fetched {len(events)} events (offset: {offset})")
                
                # Store events in database
                self._store_events(events)
                
                # Check if we've reached the limit
                if len(all_events) >= self.config.MAX_EVENTS_PER_RUN:
                    self.logger.warning(f"Reached maximum events limit: {self.config.MAX_EVENTS_PER_RUN}")
                    break
                
                offset += limit
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
                if len(events) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching events at offset {offset}: {e}")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                break
        
        self.logger.info(f"Total events fetched: {len(all_events)}")
        return all_events
    
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
        Store multiple events in the database
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
            # Store tag
            tag_record = {
                'id': tag.get('id'),
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
            relationship = {
                'event_id': event_id,
                'tag_id': tag.get('id')
            }
            self.insert_or_ignore('event_tags', relationship)
    
    def _fetch_and_store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store detailed tag information and relationships
        """
        tag_records = []
        relationships = []
        
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
            
            relationship = {
                'event_id': event_id,
                'tag_id': tag.get('id')
            }
            relationships.append(relationship)
        
        # Bulk insert
        if tag_records:
            self.bulk_insert_or_replace('tags', tag_records)
        if relationships:
            self.bulk_insert_or_replace('event_tags', relationships)
        
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
    
    def process_all_events_detailed(self):
        """
        Process all events to fetch detailed information
        """
        # Get all event IDs from database
        events = self.fetch_all("SELECT id, slug FROM events ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(events)} events for detailed information...")
        
        processed = 0
        errors = 0
        
        for event in events:
            try:
                self.fetch_event_by_id(event['id'])
                processed += 1
                
                if processed % 10 == 0:
                    self.logger.info(f"Processed {processed}/{len(events)} events")
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing event {event['id']}: {e}")
                errors += 1
        
        self.logger.info(f"Event processing complete. Processed: {processed}, Errors: {errors}")
    
    def daily_scan(self):
        """
        Perform daily scan for new events
        """
        self.logger.info("Starting daily event scan...")
        
        # Fetch active events
        active_events = self.fetch_all_events(closed=False)
        
        # Optionally fetch closed events
        if self.config.FETCH_CLOSED_EVENTS:
            closed_events = self.fetch_all_events(closed=True)
            self.logger.info(f"Fetched {len(closed_events)} closed events")
        
        # Process detailed information for new events
        self.process_all_events_detailed()
        
        self.logger.info("Daily event scan complete")
        
        return len(active_events)