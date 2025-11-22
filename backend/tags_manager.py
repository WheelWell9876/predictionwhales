"""
Tags Manager for Polymarket Terminal
Handles tag data and tag relationships operations
"""

import requests
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.database.entity.store_tags import StoreTagsManager
from backend.fetch.entity.batch.batch_tags import BatchTagsManager
from backend.fetch.entity.id.id_tags import IdTagsManager

class TagsManager:
    """Manager for tag-related operations and tag relationships"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.base_url = Config.GAMMA_API_URL
        
        # Initialize database and storage
        self.db_manager = DatabaseManager()
        self.store_manager = StoreTagsManager()
        
        # Initialize fetchers
        self.batch_fetcher = BatchTagsManager()
        self.id_fetcher = IdTagsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
    
    def fetch_all_tags(self) -> Dict:
        """
        Fetch all tags from the API and store them

        Returns:
            Dictionary with results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️  Fetching ALL TAGS")
        self.logger.info("=" * 60)

        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}

        try:
            all_tags = []
            offset = 0
            limit = 100
            batch_count = 0

            self.logger.info("Starting to fetch all tags...")
            print("🏷️  Starting to fetch tags from API...")

            while True:
                try:
                    url = f"{self.base_url}/tags"
                    params = {
                        "limit": limit,
                        "offset": offset
                    }

                    response = requests.get(
                        url,
                        params=params,
                        headers=self.config.get_api_headers(),
                        timeout=self.config.REQUEST_TIMEOUT
                    )
                    response.raise_for_status()

                    tags_list = response.json()

                    if not tags_list:
                        break

                    all_tags.extend(tags_list)
                    batch_count += 1

                    # Progress logging every 100 tags (each batch)
                    print(f"   📥 Fetched batch {batch_count}: {len(all_tags)} total tags (offset: {offset})")

                    offset += limit
                    time.sleep(self.config.RATE_LIMIT_DELAY)

                    if len(tags_list) < limit:
                        break

                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Error fetching tags at offset {offset}: {e}")
                    break

            # Store tags
            if all_tags:
                print(f"   💾 Storing {len(all_tags)} tags to database...")
                self.store_manager._store_tags(all_tags)
                result['count'] = len(all_tags)
                result['success'] = True
                print(f"   ✅ Stored {len(all_tags)} tags successfully")

            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Tags loaded: {result['count']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading tags: {e}")

        return result

    def load_tags_only(self) -> Dict:
        """
        Load only tags data - callable from data_fetcher

        Returns:
            Dictionary with load results
        """
        return self.fetch_all_tags()

    def load_tags_with_relationships(self) -> Dict:
        """
        Load tags and then load tag relationships - combined workflow

        Returns:
            Dictionary with combined results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️  Loading TAGS with RELATIONSHIPS")
        self.logger.info("=" * 60)

        combined_result = {
            'success': False,
            'tags_count': 0,
            'relationships_count': 0,
            'error': None
        }

        try:
            # First, load all tags
            tags_result = self.fetch_all_tags()
            combined_result['tags_count'] = tags_result.get('count', 0)

            if not tags_result.get('success', False):
                combined_result['error'] = tags_result.get('error', 'Failed to load tags')
                return combined_result

            # Then, load tag relationships
            self.logger.info("Now loading tag relationships...")
            relationships_result = self.fetch_all_tag_relationships()
            combined_result['relationships_count'] = relationships_result.get('count', 0)

            if not relationships_result.get('success', False):
                combined_result['error'] = relationships_result.get('error', 'Failed to load relationships')
                return combined_result

            combined_result['success'] = True
            self.logger.info(f"✅ Tags workflow complete: {combined_result['tags_count']} tags, {combined_result['relationships_count']} relationships")

        except Exception as e:
            combined_result['error'] = str(e)
            self.logger.error(f"❌ Error in tags workflow: {e}")

        return combined_result

    def delete_tags_only(self) -> Dict:
        """
        Delete tags data - callable from data_fetcher

        Returns:
            Dictionary with deletion results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TAGS Data")
        self.logger.info("=" * 60)

        result = {'success': False, 'deleted': 0, 'error': None}

        try:
            # Get current count
            before_count = self.db_manager.get_table_count('tags')

            # Delete all tags (this will cascade to relationships)
            deleted = self.db_manager.delete_records('tags', commit=True)

            result['deleted'] = before_count
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} tags")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting tags: {e}")

        return result

    def fetch_all_tag_relationships(self, num_threads: int = 10) -> Dict:
        """
        Fetch tag relationships for all events in the database

        Args:
            num_threads: Number of concurrent threads

        Returns:
            Dictionary with results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🏷️  Fetching TAG RELATIONSHIPS from Events")
        self.logger.info("=" * 60)

        start_time = time.time()
        result = {'success': False, 'count': 0, 'error': None}

        try:
            # Get all event IDs from database
            events = self.db_manager.fetch_all("SELECT id FROM events")

            if not events:
                self.logger.warning("No events found in database")
                print("⚠️  No events found in database - cannot fetch tag relationships")
                result['success'] = True
                return result

            print(f"\n🔗 Fetching TAG RELATIONSHIPS from {len(events)} events...")

            processed = 0
            relationships_stored = 0
            errors = 0

            def process_event_tags(event_id):
                try:
                    # Fetch tags for this event
                    url = f"{self.base_url}/events/{event_id}/tags"

                    response = requests.get(
                        url,
                        headers=self.config.get_api_headers(),
                        timeout=self.config.REQUEST_TIMEOUT
                    )

                    if response.status_code == 200:
                        tags_data = response.json()

                        # Extract tag relationships from response
                        relationships = []
                        for tag in tags_data:
                            if isinstance(tag, dict):
                                # Check if tag has relationship data
                                if 'tagID' in tag and 'relatedTagID' in tag:
                                    relationships.append({
                                        'tag_id': str(tag.get('tagID')),
                                        'related_tag_id': str(tag.get('relatedTagID')),
                                        'relationship_type': 'related',
                                        'strength': float(tag.get('rank', 1.0)),
                                        'created_at': datetime.now().isoformat()
                                    })

                        return relationships
                    return []

                except Exception as e:
                    self.logger.debug(f"Error processing event {event_id}: {e}")
                    return []

            # Process events concurrently
            all_relationships = []
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {executor.submit(process_event_tags, event['id']): event['id']
                          for event in events}

                for future in as_completed(futures):
                    event_id = futures[future]
                    try:
                        relationships = future.result()
                        if relationships:
                            all_relationships.extend(relationships)
                            relationships_stored += len(relationships)
                        processed += 1

                        # Progress logging every 100 events
                        if processed % 100 == 0:
                            print(f"   📊 Processed {processed}/{len(events)} events, found {relationships_stored} relationships so far")

                    except Exception as e:
                        errors += 1
                        self.logger.error(f"Error with event {event_id}: {e}")

            # Store all relationships in bulk
            unique_relationships = []
            if all_relationships:
                # Remove duplicates based on tag_id and related_tag_id
                seen = set()
                for rel in all_relationships:
                    key = (rel['tag_id'], rel['related_tag_id'])
                    if key not in seen:
                        seen.add(key)
                        unique_relationships.append(rel)

                # Store in database
                print(f"   💾 Storing {len(unique_relationships)} unique tag relationships to database...")
                with self._lock:
                    self.db_manager.bulk_insert_or_replace('tag_relationships', unique_relationships)
                print(f"   ✅ Stored {len(unique_relationships)} tag relationships successfully")
            else:
                print(f"   ⚠️  No tag relationships found in API responses")

            elapsed_time = time.time() - start_time

            result['success'] = True
            result['count'] = len(unique_relationships)

            # Final summary
            print(f"\n   📈 Tag Relationships Summary:")
            print(f"      Events processed: {processed}")
            print(f"      Total relationships found: {relationships_stored}")
            print(f"      Unique relationships stored: {result['count']}")
            print(f"      Errors: {errors}")
            print(f"      Time taken: {elapsed_time:.2f} seconds")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error fetching tag relationships: {e}")
            print(f"❌ Error fetching tag relationships: {e}")

        return result
    
    def load_tag_relationships_only(self) -> Dict:
        """
        Load only tag relationships data - callable from data_fetcher
        
        Returns:
            Dictionary with load results
        """
        return self.fetch_all_tag_relationships(num_threads=10)
    
    def delete_tag_relationships_only(self) -> Dict:
        """
        Delete tag relationships data - callable from data_fetcher
        
        Returns:
            Dictionary with deletion results
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TAG RELATIONSHIPS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Get current count
            before_count = self.db_manager.get_table_count('tag_relationships')
            
            # Delete all tag relationships
            deleted = self.db_manager.delete_records('tag_relationships', commit=True)
            
            result['deleted'] = before_count
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} tag relationships")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting tag relationships: {e}")
            
        return result
    
    def store_event_tags(self, event_id: str, tags: List):
        """
        Store event tags - delegates to store_manager

        Args:
            event_id: The event ID
            tags: List of tag dictionaries or IDs from the API
        """
        self.store_manager._store_event_tags_basic(event_id, tags)

    def store_market_tags(self, market_id: str, tags: List):
        """
        Store market tags - delegates to store_manager

        Args:
            market_id: The market ID
            tags: List of tag dictionaries or IDs from the API
        """
        self.store_manager._store_market_tags(market_id, tags)

    def get_tag_relationships_statistics(self) -> Dict:
        """Get statistics about tag relationships in the database"""
        stats = {}

        # Total relationships
        total = self.db_manager.fetch_one("SELECT COUNT(*) as count FROM tag_relationships")
        stats['total_relationships'] = total['count'] if total else 0

        # Unique tags with relationships
        unique_tags = self.db_manager.fetch_one("""
            SELECT COUNT(DISTINCT tag_id) as count FROM tag_relationships
        """)
        stats['unique_tags_with_relationships'] = unique_tags['count'] if unique_tags else 0

        return stats