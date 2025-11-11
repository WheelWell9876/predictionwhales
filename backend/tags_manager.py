"""
Tags Manager for Polymarket Terminal
Handles fetching, processing, and storing tag data and relationships
"""

import requests
import json
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
            SELECT t.label, 
                   LENGTH(et.event_ids) - LENGTH(REPLACE(et.event_ids, ',', '')) + 1 as usage_count
            FROM tags t
            JOIN event_tags et ON t.id = et.tag_id
            ORDER BY usage_count DESC
            LIMIT 10
        """)
        stats['most_used_tags'] = result
        
        return stats