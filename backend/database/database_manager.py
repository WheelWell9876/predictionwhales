"""
Base Database Manager for Polymarket Terminal
Handles common database operations with thread-safe connection management
"""

import sqlite3
import json
import logging
import threading
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager
from queue import Queue
import time

class DatabaseManager:
    """Base class for database operations with thread-safe connection management"""

    def __init__(self, db_path: str = None):
        # Import Config here to avoid circular import
        from backend.config import Config

        self.db_path = db_path or Config.DATABASE_PATH
        self.config = Config
        self.logger = self._setup_logger()
        
        # Thread-local storage for connections
        self._local = threading.local()
        
        # Global lock for write operations
        self._write_lock = threading.Lock()
        
        # Connection settings for better concurrency
        self._connection_settings = {
            'timeout': 30.0,  # Wait up to 30 seconds for locks
            'isolation_level': 'DEFERRED',  # Better concurrency
            'check_same_thread': False
        }

    def _setup_logger(self):
        """Setup logger for database operations"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)

        # Avoid duplicate handlers
        if not logger.handlers:
            # Console handler
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)

            # File handler
            fh = logging.FileHandler(self.config.LOG_FILE)
            fh.setLevel(logging.DEBUG)

            # Formatter
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            ch.setFormatter(formatter)
            fh.setFormatter(formatter)

            logger.addHandler(ch)
            logger.addHandler(fh)

        return logger

    def get_connection(self):
        """Get a thread-local connection with proper settings"""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                timeout=self._connection_settings['timeout'],
                isolation_level=self._connection_settings['isolation_level'],
                check_same_thread=self._connection_settings['check_same_thread']
            )
            self._local.conn.row_factory = sqlite3.Row
            
            # Enable WAL mode for better concurrency
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn.execute("PRAGMA cache_size=10000")
            self._local.conn.execute("PRAGMA temp_store=MEMORY")
            
        return self._local.conn

    def get_persistent_connection(self):
        """Alias for compatibility - returns thread-local connection"""
        return self.get_connection()

    def close_connection(self):
        """Close the thread-local connection"""
        if hasattr(self._local, 'conn') and self._local.conn:
            try:
                self._local.conn.close()
            except:
                pass
            self._local.conn = None

    def execute_query(self, query: str, params: tuple = None, commit: bool = False, retry_count: int = 3):
        """Execute a single query with retry logic for locks"""
        if not query.strip():  # Handle empty query for commit-only calls
            if commit:
                conn = self.get_connection()
                conn.commit()
            return None
            
        for attempt in range(retry_count):
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                if commit:
                    conn.commit()

                return cursor
                
            except sqlite3.OperationalError as e:
                if "locked" in str(e) and attempt < retry_count - 1:
                    self.logger.debug(f"Database locked, retrying... (attempt {attempt + 1}/{retry_count})")
                    time.sleep(0.1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    self.logger.error(f"Query execution failed: {e}")
                    self.logger.error(f"Query: {query}")
                    raise
            except Exception as e:
                self.logger.error(f"Query execution failed: {e}")
                self.logger.error(f"Query: {query}")
                raise

    def execute_many(self, query: str, params_list: List[tuple], commit: bool = True):
        """Execute many queries with different parameters"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.executemany(query, params_list)

            if commit:
                conn.commit()

            return cursor
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                self.logger.error(f"Database locked during batch execution, consider using smaller batches")
            raise
        except Exception as e:
            self.logger.error(f"Batch execution failed: {e}")
            raise

    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Fetch single row as dictionary"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            row = cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            self.logger.error(f"Fetch one failed: {e}")
            raise

    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """Fetch all rows as list of dictionaries"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"Fetch all failed: {e}")
            raise

    def insert_or_replace(self, table: str, data: Dict, commit: bool = True):
        """Insert or replace a single record with proper locking"""
        if not data:
            return None

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"

        with self._write_lock:
            return self.execute_query(query, tuple(data.values()), commit=commit)

    def insert_or_ignore(self, table: str, data: Dict, commit: bool = True):
        """Insert or ignore a single record with proper locking"""
        if not data:
            return None

        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"

        with self._write_lock:
            return self.execute_query(query, tuple(data.values()), commit=commit)

    def bulk_insert_or_replace(self, table: str, data_list: List[Dict], batch_size: int = 1000):
        """Bulk insert or replace multiple records with optimized batching"""
        if not data_list:
            return
        
        # Ensure all records have the same columns
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        query = f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        conn = self.get_connection()
        
        with self._write_lock:
            try:
                conn.execute("BEGIN TRANSACTION")
                
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i+batch_size]
                    params_list = [tuple(record.get(col) for col in columns) for record in batch]
                    
                    cursor = conn.cursor()
                    cursor.executemany(query, params_list)
                    
                    if (i + batch_size) % self.config.COMMIT_INTERVAL == 0:
                        conn.commit()
                        conn.execute("BEGIN TRANSACTION")
                        self.logger.debug(f"Committed {i + len(batch)} records to {table}")

                conn.commit()
                self.logger.debug(f"Bulk inserted {len(data_list)} records into {table}")
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Bulk insert failed: {e}")
                raise

    def bulk_insert_or_ignore(self, table: str, data_list: List[Dict], batch_size: int = 1000):
        """Bulk insert or ignore multiple records with optimized batching"""
        if not data_list:
            return
        
        # Ensure all records have the same columns
        columns = list(data_list[0].keys())
        placeholders = ', '.join(['?' for _ in columns])
        query = f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        conn = self.get_connection()
        
        with self._write_lock:
            try:
                conn.execute("BEGIN TRANSACTION")
                
                for i in range(0, len(data_list), batch_size):
                    batch = data_list[i:i+batch_size]
                    params_list = [tuple(record.get(col) for col in columns) for record in batch]
                    
                    cursor = conn.cursor()
                    cursor.executemany(query, params_list)
                    
                    if (i + batch_size) % self.config.COMMIT_INTERVAL == 0:
                        conn.commit()
                        conn.execute("BEGIN TRANSACTION")
                        self.logger.debug(f"Committed {i + len(batch)} records to {table}")

                conn.commit()
                self.logger.debug(f"Bulk ignored inserted {len(data_list)} records into {table}")
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Bulk insert ignore failed: {e}")
                raise

    def update_record(self, table: str, data: Dict, condition: str, params: tuple, commit: bool = True):
        """Update a record in the database"""
        if not data:
            return 0

        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {condition}"

        with self._write_lock:
            cursor = self.execute_query(query, tuple(data.values()) + params, commit=commit)
            return cursor.rowcount if cursor else 0

    def delete_records(self, table: str, condition: str = None, params: tuple = None, commit: bool = True):
        """Delete records from table"""
        if condition:
            query = f"DELETE FROM {table} WHERE {condition}"
        else:
            query = f"DELETE FROM {table}"

        with self._write_lock:
            cursor = self.execute_query(query, params, commit=commit)
            return cursor.rowcount if cursor else 0

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists"""
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """
        result = self.fetch_one(query, (table_name,))
        return result is not None

    def get_table_count(self, table_name: str) -> int:
        """Get count of records in table"""
        if not self.table_exists(table_name):
            return 0
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.fetch_one(query)
        return result['count'] if result else 0

    # def reset_database(self):
    #     """Reset entire database (clear all data but keep schema)"""
    #     tables = [
    #         'events', 'markets', 'tags', 'series', 'collections',
    #         'event_tags', 'market_tags', 'series_events', 'series_collections',
    #         'series_tags', 'collection_tags', 'tag_relationships',
    #         'event_live_volume', 'market_open_interest',
    #         'users', 'user_positions_current', 'user_positions_closed',
    #         'user_trades', 'user_values', 'market_holders',
    #         'comments', 'comment_reactions', 'user_activity', 'transactions'
    #     ]

    #     conn = self.get_connection()
        
    #     with self._write_lock:
    #         try:
    #             conn.execute("BEGIN TRANSACTION")
                
    #             for table in tables:
    #                 if self.table_exists(table):
    #                     conn.execute(f"DELETE FROM {table}")
    #                     self.logger.info(f"Cleared table: {table}")

    #             conn.commit()
    #             conn.execute("VACUUM")  # Reclaim space

    #             self.logger.info("Database reset complete")
                
    #         except Exception as e:
    #             conn.rollback()
    #             self.logger.error(f"Database reset failed: {e}")
    #             raise

    # def remove_closed_events(self):
    #     """Remove all closed/inactive events and their associated data"""
    #     self.logger.info("🧹 Removing closed/inactive events and associated data...")
        
    #     conn = self.get_connection()
        
    #     with self._write_lock:
    #         try:
    #             cursor = conn.cursor()
                
    #             # Get count before deletion
    #             cursor.execute("SELECT COUNT(*) FROM events WHERE closed = 1 OR active = 0")
    #             closed_count = cursor.fetchone()[0]
                
    #             if closed_count == 0:
    #                 self.logger.info("No closed events to remove")
    #                 return 0
                
    #             self.logger.info(f"Found {closed_count} closed/inactive events to remove")
                
    #             # Get IDs of closed events
    #             cursor.execute("SELECT id FROM events WHERE closed = 1 OR active = 0")
    #             closed_event_ids = [row[0] for row in cursor.fetchall()]
                
    #             if not closed_event_ids:
    #                 return 0
                
    #             # Create placeholders for SQL IN clause
    #             placeholders = ','.join('?' * len(closed_event_ids))
                
    #             conn.execute("BEGIN TRANSACTION")
                
    #             # Delete markets associated with closed events
    #             cursor.execute(f"SELECT COUNT(*) FROM markets WHERE event_id IN ({placeholders})", closed_event_ids)
    #             markets_count = cursor.fetchone()[0]
                
    #             cursor.execute(f"DELETE FROM markets WHERE event_id IN ({placeholders})", closed_event_ids)
    #             self.logger.info(f"  Deleted {markets_count} markets associated with closed events")
                
    #             # Delete comments associated with closed events
    #             cursor.execute(f"SELECT COUNT(*) FROM comments WHERE event_id IN ({placeholders})", closed_event_ids)
    #             comments_count = cursor.fetchone()[0]
                
    #             cursor.execute(f"DELETE FROM comments WHERE event_id IN ({placeholders})", closed_event_ids)
    #             self.logger.info(f"  Deleted {comments_count} comments associated with closed events")
                
    #             # Delete the closed events themselves
    #             cursor.execute(f"DELETE FROM events WHERE id IN ({placeholders})", closed_event_ids)
                
    #             conn.commit()
                
    #             # Vacuum to reclaim space
    #             self.logger.info("  Running VACUUM to reclaim space...")
    #             conn.execute("VACUUM")
                
    #             self.logger.info(f"✅ Successfully removed {closed_count} closed events and associated data")
                
    #             return closed_count
                
    #         except Exception as e:
    #             conn.rollback()
    #             self.logger.error(f"Remove closed events failed: {e}")
    #             raise

    # def get_statistics(self) -> Dict[str, int]:
    #     """Get database statistics"""
    #     stats = {}
    #     tables = [
    #         'events', 'markets', 'tags', 'series', 'collections',
    #         'event_tags', 'market_tags', 'users', 'transactions',
    #         'user_positions_current', 'user_positions_closed',
    #         'user_trades', 'user_activity', 'user_values'
    #     ]

    #     for table in tables:
    #         if self.table_exists(table):
    #             stats[table] = self.get_table_count(table)

    #     return stats

    # def backup_database(self, backup_path: str = None):
    #     """Create a backup of the database"""
    #     import shutil
    #     from datetime import datetime

    #     if not backup_path:
    #         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #         backup_path = f"{self.db_path}.backup_{timestamp}"

    #     # Close all connections before backup
    #     self.close_connection()

    #     try:
    #         shutil.copy2(self.db_path, backup_path)
    #         self.logger.info(f"Database backed up to: {backup_path}")
    #     except Exception as e:
    #         self.logger.error(f"Backup failed: {e}")
    #         raise

    #     return backup_path

    # def __del__(self):
    #     """Cleanup connections on object destruction"""
    #     self.close_connection()

    # def __enter__(self):
    #     """Context manager entry"""
    #     return self

    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     """Context manager exit - ensure connection is closed"""
    #     self.close_connection()