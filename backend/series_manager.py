"""
Series Manager for Polymarket Terminal
Handles fetching, processing, and storing series data
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from .database_manager import DatabaseManager
from .config import Config

class SeriesManager(DatabaseManager):
    """Manager for series-related operations"""
    
    def __init__(self):
        super().__init__()
        from .config import Config
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
    
    def fetch_all_series(self, limit: int = 100) -> List[Dict]:
        """
        Fetch all series from the API
        Used for initial data load and daily scans
        """
        all_series = []
        offset = 0
        
        self.logger.info("Starting to fetch all series...")
        
        while True:
            try:
                url = f"{self.base_url}/series"
                params = {
                    "limit": limit,
                    "offset": offset,
                    "order": "volume",
                    "ascending": "false",
                    "include_chat": "true"
                }
                
                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
                response.raise_for_status()
                
                series_list = response.json()
                
                if not series_list:
                    break
                
                all_series.extend(series_list)
                
                # Store series
                self._store_series_list(series_list)
                
                self.logger.info(f"Fetched {len(series_list)} series (offset: {offset})")
                
                offset += limit
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
                if len(series_list) < limit:
                    break
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error fetching series at offset {offset}: {e}")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                break
        
        self.logger.info(f"Total series fetched: {len(all_series)}")
        return all_series
    
    def fetch_series_by_id(self, series_id: str) -> Optional[Dict]:
        """
        Fetch detailed information for a specific series
        """
        try:
            url = f"{self.base_url}/series/{series_id}"
            params = {"include_chat": "true"}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            
            series = response.json()
            
            # Store detailed series
            self._store_series_detailed(series)
            
            # Process events in series
            if 'events' in series:
                self._store_series_events(series_id, series['events'])
            
            # Process collections in series
            if 'collections' in series:
                self._store_series_collections(series_id, series['collections'])
            
            return series
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching series {series_id}: {e}")
            return None
    
    def _store_series_list(self, series_list: List[Dict]):
        """
        Store multiple series records
        """
        series_records = []
        
        for series in series_list:
            record = self._prepare_series_record(series)
            series_records.append(record)
            
            # Process events if present
            if 'events' in series:
                self._store_series_events(series['id'], series['events'])
            
            # Process collections if present
            if 'collections' in series:
                self._store_series_collections(series['id'], series['collections'])
        
        # Bulk insert series
        if series_records:
            self.bulk_insert_or_replace('series', series_records)
            self.logger.debug(f"Stored {len(series_records)} series")
    
    def _store_series_detailed(self, series: Dict):
        """
        Store detailed series information
        """
        record = self._prepare_series_record(series)
        self.insert_or_replace('series', record)
        self.logger.debug(f"Stored detailed series: {series.get('id')}")
    
    def _prepare_series_record(self, series: Dict) -> Dict:
        """
        Prepare a series record for database insertion
        """
        return {
            'id': series.get('id'),
            'ticker': series.get('ticker'),
            'slug': series.get('slug'),
            'title': series.get('title'),
            'subtitle': series.get('subtitle'),
            'series_type': series.get('seriesType'),
            'recurrence': series.get('recurrence'),
            'description': series.get('description'),
            'image': series.get('image'),
            'icon': series.get('icon'),
            'layout': series.get('layout'),
            'active': series.get('active'),
            'closed': series.get('closed'),
            'archived': series.get('archived'),
            'new': series.get('new'),
            'featured': series.get('featured'),
            'restricted': series.get('restricted'),
            'is_template': series.get('isTemplate'),
            'template_variables': json.dumps(series.get('templateVariables')) if series.get('templateVariables') else None,
            'published_at': series.get('publishedAt'),
            'created_by': series.get('createdBy'),
            'updated_by': series.get('updatedBy'),
            'created_at': series.get('createdAt'),
            'updated_at': series.get('updatedAt'),
            'comments_enabled': series.get('commentsEnabled'),
            'competitive': series.get('competitive'),
            'volume_24hr': series.get('volume24hr'),
            'volume': series.get('volume'),
            'liquidity': series.get('liquidity'),
            'start_date': series.get('startDate'),
            'pyth_token_id': series.get('pythTokenID'),
            'cg_asset_name': series.get('cgAssetName'),
            'score': series.get('score'),
            'fetched_at': datetime.now().isoformat()
        }
    
    def _store_series_events(self, series_id: str, events: List[Dict]):
        """
        Store events associated with a series
        """
        event_records = []

        for event in events:
            # Store event data
            event_record = {
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
                'volume_1wk': event.get('volume1wk'),
                'volume_1mo': event.get('volume1mo'),
                'volume_1yr': event.get('volume1yr'),
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
                'automatically_active': event.get('automaticallyActive'),
                'neg_risk_augmented': event.get('negRiskAugmented'),
                'created_at': event.get('createdAt'),
                'updated_at': event.get('updatedAt'),
                'fetched_at': datetime.now().isoformat()
            }
            event_records.append(event_record)

            # Store event tags if present
            if 'tags' in event:
                self._store_event_tags(event['id'], event['tags'])

        # Bulk insert events
        if event_records:
            self.bulk_insert_or_replace('events', event_records)

        # Store series-event relationship
        conn = self.get_persistent_connection()
        cursor = conn.cursor()

        event_ids = [event.get('id') for event in events]

        cursor.execute("SELECT event_ids FROM series_events WHERE series_id = ?", (series_id,))
        result = cursor.fetchone()

        if result:
            existing_event_ids = json.loads(result[0])
            for event_id in event_ids:
                if event_id not in existing_event_ids:
                    existing_event_ids.append(event_id)
            cursor.execute(
                "UPDATE series_events SET event_ids = ? WHERE series_id = ?",
                (json.dumps(existing_event_ids), series_id)
            )
        else:
            cursor.execute(
                "INSERT INTO series_events (series_id, event_ids) VALUES (?, ?)",
                (series_id, json.dumps(event_ids))
            )

        conn.commit()

        self.logger.debug(f"Stored {len(events)} events for series {series_id}")

    def _store_series_collections(self, series_id: str, collections: List[Dict]):
        """
        Store collections associated with a series
        """
        collection_records = []
        relationships = []

        for collection in collections:
            # Store collection data
            collection_record = {
                'id': collection.get('id'),
                'ticker': collection.get('ticker'),
                'slug': collection.get('slug'),
                'title': collection.get('title'),
                'subtitle': collection.get('subtitle'),
                'collection_type': collection.get('collectionType'),
                'description': collection.get('description'),
                'tags': json.dumps(collection.get('tags')) if collection.get('tags') else None,
                'image': collection.get('image'),
                'icon': collection.get('icon'),
                'header_image': collection.get('headerImage'),
                'layout': collection.get('layout'),
                'active': collection.get('active'),
                'closed': collection.get('closed'),
                'archived': collection.get('archived'),
                'new': collection.get('new'),
                'featured': collection.get('featured'),
                'restricted': collection.get('restricted'),
                'is_template': collection.get('isTemplate'),
                'template_variables': json.dumps(collection.get('templateVariables')) if collection.get('templateVariables') else None,
                'published_at': collection.get('publishedAt'),
                'created_by': collection.get('createdBy'),
                'updated_by': collection.get('updatedBy'),
                'created_at': collection.get('createdAt'),
                'updated_at': collection.get('updatedAt'),
                'comments_enabled': collection.get('commentsEnabled'),
                'comment_count': collection.get('commentCount'),
                'fetched_at': datetime.now().isoformat()
            }
            collection_records.append(collection_record)

            # Create series-collection relationship
            relationship = {
                'series_id': series_id,
                'collection_id': collection.get('id')
            }
            relationships.append(relationship)

            # Store collection tags if present
            if 'tags' in collection and isinstance(collection['tags'], list):
                self._store_collection_tags(collection['id'], collection['tags'])

        # Bulk insert
        if collection_records:
            self.bulk_insert_or_replace('collections', collection_records)
        if relationships:
            self.bulk_insert_or_replace('series_collections', relationships)

        self.logger.debug(f"Stored {len(collections)} collections for series {series_id}")

    def _store_event_tags(self, event_id: str, tags: List[Dict]):
        """
        Store tags for an event in a series
        """
        tag_records = []

        for tag in tags:
            tag_record = {
                'id': tag.get('id'),
                'label': tag.get('label'),
                'slug': tag.get('slug'),
                'force_show': tag.get('forceShow', False),
                'is_carousel': tag.get('isCarousel', False),
                'created_at': tag.get('createdAt'),
                'updated_at': tag.get('updatedAt')
            }
            tag_records.append(tag_record)

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
    
    def _store_collection_tags(self, collection_id: str, tags: List[Dict]):
        """
        Store tags for a collection
        """
        relationships = []
        
        for tag in tags:
            # Store tag if it has complete data
            if isinstance(tag, dict) and 'id' in tag:
                tag_record = {
                    'id': tag.get('id'),
                    'label': tag.get('label'),
                    'slug': tag.get('slug'),
                    'created_at': tag.get('createdAt'),
                    'updated_at': tag.get('updatedAt')
                }
                self.insert_or_ignore('tags', tag_record)
                
                relationship = {
                    'collection_id': collection_id,
                    'tag_id': tag.get('id')
                }
                relationships.append(relationship)
        
        if relationships:
            self.bulk_insert_or_replace('collection_tags', relationships)
    
    def process_all_series_detailed(self):
        """
        Process all series to fetch detailed information
        """
        # Get all series IDs from database
        series_list = self.fetch_all("SELECT id, slug FROM series ORDER BY volume DESC")
        
        self.logger.info(f"Processing {len(series_list)} series for detailed information...")
        
        processed = 0
        errors = 0
        
        for series in series_list:
            try:
                self.fetch_series_by_id(series['id'])
                processed += 1
                
                if processed % 10 == 0:
                    self.logger.info(f"Processed {processed}/{len(series_list)} series")
                
                # Rate limiting
                time.sleep(self.config.RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing series {series['id']}: {e}")
                errors += 1
        
        self.logger.info(f"Series processing complete. Processed: {processed}, Errors: {errors}")
    
    def daily_scan(self):
        """
        Perform daily scan for series updates
        """
        if not self.config.FETCH_SERIES:
            self.logger.info("Series fetching disabled")
            return 0
        
        self.logger.info("Starting daily series scan...")
        
        # Fetch all series
        all_series = self.fetch_all_series()
        
        # Process detailed information
        self.process_all_series_detailed()
        
        self.logger.info(f"Daily series scan complete. Total series: {len(all_series)}")
        
        return len(all_series)