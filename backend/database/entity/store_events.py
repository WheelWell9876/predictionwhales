"""
Store events
Handles storage operations for events in the database
"""

from datetime import datetime
from typing import Dict, List
import logging

class StoreEvents:
    """Handles storage operations for events"""

    def __init__(self, database_manager):
        self.db_manager = database_manager
        self.config = database_manager.config
        self.logger = logging.getLogger(self.__class__.__name__)

    def store_events_batch(self, events: List[Dict]):
        """
        Store multiple events in the database
        
        Args:
            events: List of event dictionaries to store
        """
        if not events:
            return
            
        event_records = []
        events_with_tags = []  # Track which events have tags to process later
        
        for event in events:
            record = self._prepare_event_record(event)
            event_records.append(record)
            
            # Store the tags for later processing (AFTER events are inserted)
            if 'tags' in event and event['tags']:
                events_with_tags.append((event['id'], event['tags']))
        
        # FIRST: Bulk insert all events
        if event_records:
            self.db_manager.bulk_insert_or_replace('events', event_records)
            self.logger.info(f"Stored {len(event_records)} events in database")
        
        # SECOND: Now that events exist, store their tags
        for event_id, tags in events_with_tags:
            self.store_event_tags(event_id, tags)

    def store_event_detailed(self, event: Dict):
        """
        Store detailed event information
        
        Args:
            event: Event dictionary with detailed information
        """
        if not event:
            return
            
        record = self._prepare_event_record(event)
        self.db_manager.insert_or_replace('events', record)
        self.logger.debug(f"Stored detailed event: {event.get('id')}")
        
        # Store tags if present
        if 'tags' in event and event['tags']:
            self.store_event_tags(event['id'], event['tags'])

    def store_event_tags(self, event_id: str, tags: List):
        """
        Store tags for an event with detailed debugging
        
        Args:
            event_id: The event ID
            tags: List of tags (can be strings or dictionaries)
        """
        if not tags or not self.config.FETCH_TAGS:
            return
        
        self.logger.debug(f"=== STORE EVENT TAGS DEBUG ===")
        self.logger.debug(f"Event ID: {event_id}")
        self.logger.debug(f"Raw tags received (first 3): {tags[:3]}")
        
        tag_records = []
        event_tag_records = []
        
        for idx, tag in enumerate(tags):
            self.logger.debug(f"Processing tag {idx+1}: Type={type(tag)}, Value={tag}")
            
            # Handle both string tags and tag objects
            if isinstance(tag, str):
                tag_id = tag
                tag_slug = tag
                tag_label = tag
            elif isinstance(tag, dict):
                tag_id = tag.get('id', '')
                tag_slug = tag.get('slug', '')
                tag_label = tag.get('label', '')
                
                self.logger.debug(f"  Extracted - ID: {tag_id}, Slug: {tag_slug}, Label: {tag_label}")
            else:
                self.logger.warning(f"  Skipping unknown tag type: {type(tag)}")
                continue
                
            if tag_id:
                # First, insert/update the tag itself into the tags table
                tag_record = {
                    'id': tag_id,
                    'slug': tag_slug,
                    'label': tag_label,
                    'force_show': int(tag.get('forceShow', False)) if isinstance(tag, dict) else 0,
                    'force_hide': 0,  # Not provided in API
                    'is_carousel': int(tag.get('isCarousel', False)) if isinstance(tag, dict) else 0,
                    'published_at': tag.get('publishedAt') if isinstance(tag, dict) else None,
                    'created_by': None,  # Not provided in API
                    'updated_by': None,  # Not provided in API
                    'created_at': tag.get('createdAt') if isinstance(tag, dict) else None,
                    'updated_at': tag.get('updatedAt') if isinstance(tag, dict) else None,
                    'fetched_at': datetime.now().isoformat()
                }
                tag_records.append(tag_record)
                
                # Prepare the event-tag relationship record
                event_tag_record = {
                    'event_id': event_id,
                    'tag_id': tag_id,
                    'tag_slug': tag_slug
                }
                event_tag_records.append(event_tag_record)
        
        self.logger.debug(f"Prepared {len(tag_records)} tag records and {len(event_tag_records)} event_tag records")
        
        # First insert the tags themselves (ignore if they already exist)
        if tag_records:
            self.logger.debug("Inserting tags into tags table...")
            self.db_manager.bulk_insert_or_ignore('tags', tag_records)
            self.logger.debug(f"Tags insert complete")
        
        # Then insert the event-tag relationships
        if event_tag_records:
            self.logger.debug("Inserting relationships into event_tags table...")
            self.logger.debug(f"First event_tag record: {event_tag_records[0]}")
            self.db_manager.bulk_insert_or_ignore('event_tags', event_tag_records)
            self.logger.debug(f"Event_tags insert complete")

    def store_event_live_volume(self, event_id: str, volume_data: Dict):
        """
        Store live volume data for an event
        
        Args:
            event_id: The event ID
            volume_data: Volume data dictionary
        """
        if not volume_data:
            return
            
        record = {
            'event_id': event_id,
            'volume': volume_data.get('volume'),
            'volume_24hr': volume_data.get('volume24hr'),
            'liquidity': volume_data.get('liquidity'),
            'fetched_at': datetime.now().isoformat()
        }
        
        self.db_manager.insert_or_replace('event_live_volume', record)
        self.logger.debug(f"Stored live volume for event {event_id}")

    def _prepare_event_record(self, event: Dict) -> Dict:
        """
        Prepare event data for database storage
        
        Args:
            event: Raw event dictionary from API
            
        Returns:
            Formatted record dictionary for database
        """
        return {
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

    def remove_closed_events(self) -> int:
        """
        Remove all closed events from the database
        
        Returns:
            Number of events removed
        """
        self.logger.info("Removing closed events from database...")
        
        # Count closed events
        result = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
        closed_count = result['count'] if result else 0
        
        if closed_count > 0:
            # Delete closed events and their tags
            self.db_manager.delete_records('event_tags', 'event_id IN (SELECT id FROM events WHERE closed = 1)')
            deleted = self.db_manager.delete_records('events', 'closed = 1', commit=True)
            self.logger.info(f"Removed {deleted} closed events")
            return deleted
        else:
            self.logger.info("No closed events to remove")
            return 0