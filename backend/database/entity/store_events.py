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
        self._event_counter = 0
        self._tag_progress_counter = 0

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
        events_with_images = []  # Track events with image optimization data

        for event in events:
            record = self._prepare_event_record(event)
            event_records.append(record)

            # Store the tags for later processing (AFTER events are inserted)
            if 'tags' in event and event['tags']:
                events_with_tags.append((event['id'], event['tags']))

            # Collect image optimization data
            if 'imageOptimized' in event and event['imageOptimized']:
                events_with_images.append((event['id'], event['imageOptimized'], 'image'))
            if 'iconOptimized' in event and event['iconOptimized']:
                events_with_images.append((event['id'], event['iconOptimized'], 'icon'))
            if 'featuredImageOptimized' in event and event['featuredImageOptimized']:
                events_with_images.append((event['id'], event['featuredImageOptimized'], 'featuredImage'))

        # Bulk insert all events
        if event_records:
            self.db_manager.bulk_insert_or_replace('events', event_records)
            self._event_counter += len(event_records)

            # Log progress every 100 events
            if self._event_counter % 100 == 0:
                self.logger.info(f"Stored {self._event_counter} events")

        # Now that events exist, store their tags
        for event_id, tags in events_with_tags:
            self.store_event_tags(event_id, tags)

        # Store image optimization data
        if events_with_images:
            self._store_image_optimized_batch(events_with_images)

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
        Store tags for an event
        
        Args:
            event_id: The event ID
            tags: List of tags (can be strings or dictionaries)
        """
        if not tags or not self.config.FETCH_TAGS:
            return
        
        tag_records = []
        event_tag_records = []
        
        for tag in tags:
            # Handle both string tags and tag objects
            if isinstance(tag, str):
                tag_id = tag
                tag_slug = tag
                tag_label = tag
            elif isinstance(tag, dict):
                tag_id = tag.get('id', '')
                tag_slug = tag.get('slug', '')
                tag_label = tag.get('label', '')
            else:
                continue
                
            if tag_id:
                # First, insert/update the tag itself into the tags table
                tag_record = {
                    'id': tag_id,
                    'slug': tag_slug,
                    'label': tag_label,
                    'force_show': int(tag.get('forceShow', False)) if isinstance(tag, dict) else 0,
                    'force_hide': int(tag.get('forceHide', False)) if isinstance(tag, dict) else 0,
                    'is_carousel': int(tag.get('isCarousel', False)) if isinstance(tag, dict) else 0,
                    'published_at': tag.get('publishedAt') if isinstance(tag, dict) else None,
                    'created_by': tag.get('createdBy') if isinstance(tag, dict) else None,
                    'updated_by': tag.get('updatedBy') if isinstance(tag, dict) else None,
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
        
        # Insert tags (ignore if they already exist)
        if tag_records:
            self.db_manager.bulk_insert_or_ignore('tags', tag_records)
        
        # Insert event-tag relationships
        if event_tag_records:
            self.db_manager.bulk_insert_or_ignore('event_tags', event_tag_records)
            
        # Update progress tracking if we have it
        if hasattr(self, '_tag_progress_counter'):
            self._tag_progress_counter += len(event_tag_records)
            if self._tag_progress_counter % 100 == 0:
                self.logger.info(f"Stored {self._tag_progress_counter} event-tag relationships")

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
        ONLY INCLUDING FIELDS THAT EXIST IN THE SCHEMA
        
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
            'subtitle': event.get('subtitle'),
            'description': event.get('description'),
            'resolution_source': event.get('resolutionSource'),
            'start_date': event.get('startDate'),
            'creation_date': event.get('creationDate'),
            'end_date': event.get('endDate'),
            'image': event.get('image'),
            'icon': event.get('icon'),
            'featured_image': event.get('featuredImage'),
            'active': int(event.get('active', True)),
            'closed': int(event.get('closed', False)),
            'archived': int(event.get('archived', False)),
            'new': int(event.get('new', False)),
            'featured': int(event.get('featured', False)),
            'restricted': int(event.get('restricted', False)),
            'is_template': int(event.get('isTemplate', False)),
            'template_variables': event.get('templateVariables'),
            'liquidity': event.get('liquidity'),
            'liquidity_amm': event.get('liquidityAmm'),
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
            'tweet_count': event.get('tweetCount'),
            'enable_order_book': int(event.get('enableOrderBook', False)),
            'cyom': int(event.get('cyom', False)),
            'show_all_outcomes': int(event.get('showAllOutcomes', False)),
            'show_market_images': int(event.get('showMarketImages', False)),
            'enable_neg_risk': int(event.get('enableNegRisk', False)),
            'neg_risk_market_id': event.get('negRiskMarketID'),
            'neg_risk_fee_bips': event.get('negRiskFeeBips'),
            'automatically_resolved': int(event.get('automaticallyResolved', False)),
            'automatically_active': int(event.get('automaticallyActive', False)),
            'closed_time': event.get('closedTime'),
            'event_date': event.get('eventDate'),
            'start_time': event.get('startTime'),
            'event_week': event.get('eventWeek'),
            'series_slug': event.get('seriesSlug'),
            'score': event.get('score'),
            'elapsed': event.get('elapsed'),
            'period': event.get('period'),
            'live': int(event.get('live', False)),
            'ended': int(event.get('ended', False)),
            'finished_timestamp': event.get('finishedTimestamp'),
            'gmp_chart_mode': event.get('gmpChartMode'),
            'estimate_value': int(event.get('estimateValue', False)),
            'cant_estimate': int(event.get('cantEstimate', False)),
            'estimated_value': event.get('estimatedValue'),
            'carousel_map': event.get('carouselMap'),
            'pending_deployment': int(event.get('pendingDeployment', False)),
            'deploying': int(event.get('deploying', False)),
            'deploying_timestamp': event.get('deployingTimestamp'),
            'scheduled_deployment_timestamp': event.get('scheduledDeploymentTimestamp'),
            'game_status': event.get('gameStatus'),
            'spreads_main_line': event.get('spreadsMainLine'),
            'totals_main_line': event.get('totalsMainLine'),
            'published_at': event.get('publishedAt'),
            'created_by': event.get('createdBy'),
            'updated_by': event.get('updatedBy'),
            'created_at': event.get('createdAt'),
            'updated_at': event.get('updatedAt'),
            'fetched_at': datetime.now().isoformat()
        }

    def _store_image_optimized_batch(self, image_data: list):
        """
        Store image optimization data in batch for events

        Args:
            image_data: List of tuples (event_id, img_data, field_type)
        """
        image_records = []

        for event_id, img_data, field_type in image_data:
            if img_data:
                image_records.append({
                    'id': img_data.get('id'),
                    'image_url_source': img_data.get('imageUrlSource'),
                    'image_url_optimized': img_data.get('imageUrlOptimized'),
                    'image_size_kb_source': self._safe_float(img_data.get('imageSizeKbSource')),
                    'image_size_kb_optimized': self._safe_float(img_data.get('imageSizeKbOptimized')),
                    'image_optimized_complete': int(img_data.get('imageOptimizedComplete', False)),
                    'image_optimized_last_updated': img_data.get('imageOptimizedLastUpdated'),
                    'rel_id': img_data.get('relID'),
                    'field': img_data.get('field', field_type),
                    'relname': img_data.get('relname'),
                    'entity_type': 'event',
                    'entity_id': event_id
                })

        if image_records:
            self.db_manager.bulk_insert_or_ignore('image_optimized', image_records)
            self.logger.debug(f"Stored {len(image_records)} image optimization records for events")

    def _safe_float(self, value):
        """Safely convert value to float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

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