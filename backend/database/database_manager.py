#!/usr/bin/env python3
"""
Database Manager for Polymarket Terminal
Handles all database operations with SQLite
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.config import Config

class DatabaseManager:
    """Manager for all database operations"""
    
    def __init__(self, db_path: str = None):
        """Initialize database connection and create tables if needed"""
        self.db_path = db_path or Config.DATABASE_PATH
        self.config = Config
        self.logger = self._setup_logger()
        
        # Ensure database directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Initializing database at: {self.db_path}")
        
        # Initialize database schema
        self.initialize_schema()
        
        # Verify all tables are created
        self.verify_tables()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for database operations"""
        logger = logging.getLogger('DatabaseManager')
        logger.setLevel(logging.DEBUG)  # Set to DEBUG for better debugging
        
        # Console handler
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def get_connection(self):
        """Get database connection with proper settings"""
        conn = sqlite3.connect(
            self.db_path, 
            timeout=30.0,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        
        # Set optimal pragmas
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA cache_size = -64000")
        conn.execute("PRAGMA temp_store = MEMORY")
        
        return conn
    
    def close_connection(self):
        """Close database connection - for compatibility with managers that expect this method"""
        # SQLite connections in this implementation are closed after each operation
        # This method exists for compatibility with other managers
        self.logger.debug("close_connection called (no-op for SQLite with per-operation connections)")
    pass
    
    def initialize_schema(self):
        """Initialize database schema from database_schema.py"""
        self.logger.info("Initializing database schema...")
        
        try:
            # Import the schema
            from backend.database.database_schema import get_schema
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get the full schema
            schema_sql = get_schema()
            
            # Split by semicolons to execute each statement separately
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            self.logger.info(f"Executing {len(statements)} SQL statements...")
            
            # Track what tables we're creating
            tables_created = []
            indexes_created = []
            
            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        # Debug: Log what type of statement this is
                        if 'CREATE TABLE' in statement:
                            # Extract table name for logging
                            import re
                            match = re.search(r'CREATE TABLE IF NOT EXISTS (\w+)', statement)
                            if match:
                                table_name = match.group(1)
                                tables_created.append(table_name)
                                self.logger.debug(f"Creating table: {table_name}")
                        elif 'CREATE INDEX' in statement:
                            import re
                            match = re.search(r'CREATE INDEX IF NOT EXISTS (\w+)', statement)
                            if match:
                                index_name = match.group(1)
                                indexes_created.append(index_name)
                                self.logger.debug(f"Creating index: {index_name}")
                        
                        cursor.execute(statement)
                        
                    except sqlite3.Error as e:
                        self.logger.error(f"Error executing statement {i+1}: {e}")
                        self.logger.error(f"Statement: {statement[:100]}...")
                        raise
            
            conn.commit()
            
            # Log summary
            self.logger.info(f"✅ Schema initialization complete!")
            self.logger.info(f"   Tables created: {len(tables_created)}")
            self.logger.info(f"   Indexes created: {len(indexes_created)}")
            
            # Log specific tables for debugging
            if tables_created:
                self.logger.debug(f"Tables: {', '.join(sorted(tables_created))}")
            
            # Specifically check for event_tags
            if 'event_tags' in tables_created:
                self.logger.info("✅ event_tags table successfully created!")
            else:
                self.logger.warning("⚠️ event_tags table not found in creation list!")
            
            conn.close()
            
        except ImportError as e:
            self.logger.error(f"Failed to import schema: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def verify_tables(self):
        """Verify all required tables exist"""
        self.logger.info("Verifying database tables...")
        
        required_tables = [
            'events', 'markets', 'collections', 'series', 'tags', 'users', 'comments',
            'event_tags', 'market_tags', 'series_tags', 'collection_tags',  # Relationship tables
            'series_events', 'series_collections', 'tag_relationships',
            'event_live_volume', 'market_open_interest', 'market_holders',
            'user_activity', 'user_trades', 'user_positions_current', 'user_positions_closed',
            'user_values', 'transactions', 'comment_reactions'
        ]
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get all existing tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
            existing_tables = {row[0] for row in cursor.fetchall()}
            
            # Check each required table
            missing_tables = []
            for table in required_tables:
                if table not in existing_tables:
                    missing_tables.append(table)
                    self.logger.error(f"❌ Missing table: {table}")
                else:
                    self.logger.debug(f"✅ Table exists: {table}")
            
            if missing_tables:
                self.logger.error(f"Missing {len(missing_tables)} tables: {', '.join(missing_tables)}")
                # Try to create missing tables by re-initializing schema
                self.logger.info("Attempting to recreate missing tables...")
                self.initialize_schema()
            else:
                self.logger.info(f"✅ All {len(required_tables)} required tables verified!")
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error verifying tables: {e}")
            raise
    
    def execute(self, query: str, params: tuple = None) -> int:
        """Execute a query and return affected rows"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            self.logger.error(f"Query: {query[:200]}...")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def executemany(self, query: str, params_list: List[tuple]) -> int:
        """Execute many queries and return total affected rows"""
        if not params_list:
            return 0
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            self.logger.error(f"Database error in executemany: {e}")
            self.logger.error(f"Query: {query[:200]}...")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Fetch single row as dictionary"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """Fetch all rows as list of dictionaries"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def insert_or_replace(self, table: str, data: Dict) -> int:
        """Insert or replace a single record"""
        if not data:
            return 0
        
        columns = list(data.keys())
        placeholders = ','.join(['?' for _ in columns])
        query = f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        values = tuple(data.get(col) for col in columns)
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            self.logger.error(f"Insert or replace error in {table}: {e}")
            self.logger.error(f"Data: {data}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def insert_or_ignore(self, table: str, data: Dict) -> int:
        """Insert or ignore a single record"""
        if not data:
            return 0
        
        columns = list(data.keys())
        placeholders = ','.join(['?' for _ in columns])
        query = f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        values = tuple(data.get(col) for col in columns)
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query, values)
            conn.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            self.logger.error(f"Insert or ignore error in {table}: {e}")
            self.logger.error(f"Data: {data}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def bulk_insert_or_replace(self, table: str, data: List[Dict], batch_size: int = 100) -> int:
        """Bulk insert or replace data with reduced logging"""
        if not data:
            return 0
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        query = f"INSERT OR REPLACE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        total_inserted = 0
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            
            # Process in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                params_list = [tuple(record.get(col) for col in columns) for record in batch]
                
                cursor.executemany(query, params_list)
                total_inserted += cursor.rowcount
                
                # Commit after each batch
                conn.commit()
                
                # Only log every 1000 records
                if (i + batch_size) % 1000 == 0:
                    self.logger.debug(f"Progress: {i + batch_size} records processed for {table}")
            
            # Final summary
            self.logger.info(f"Bulk inserted/replaced {total_inserted} records into {table}")
            return total_inserted
            
        except sqlite3.Error as e:
            self.logger.error(f"Bulk insert/replace error in {table}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def bulk_insert_or_ignore(self, table: str, data: List[Dict], batch_size: int = 100) -> int:
        """Bulk insert or ignore data with reduced logging"""
        if not data:
            return 0
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        query = f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        total_inserted = 0
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            
            # Process in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                params_list = [tuple(record.get(col) for col in columns) for record in batch]
                
                cursor.executemany(query, params_list)
                total_inserted += cursor.rowcount
                
                # Commit after each batch
                conn.commit()
                
                # Only log every 1000 records to file, not console
                if (i + batch_size) % 1000 == 0:
                    self.logger.debug(f"Progress: {i + batch_size} records processed for {table}")
            
            # Final summary
            if total_inserted > 0:
                self.logger.info(f"Bulk inserted {total_inserted} records into {table}")
            
            return total_inserted
            
        except sqlite3.Error as e:
            # Only log errors in detail
            self.logger.error(f"Bulk insert error in {table}: {e}")
            
            # Enhanced error debugging only for failures
            if "FOREIGN KEY" in str(e):
                self.logger.error("=== FOREIGN KEY DEBUG ===")
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                fk_info = cursor.fetchall()
                self.logger.error(f"Foreign keys for {table}: {[dict(row) for row in fk_info]}")
                
                # Log sample of problematic data
                if data:
                    self.logger.error(f"First problematic record: {data[0]}")
            
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def delete_records(self, table: str, where_clause: str = None, params: tuple = None, commit: bool = True) -> int:
        """
        Delete records from a table
        
        Args:
            table: Table name
            where_clause: Optional WHERE clause (without the WHERE keyword)
            params: Optional parameters for the where clause
            commit: Whether to commit immediately
            
        Returns:
            Number of deleted records
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            if where_clause:
                query = f"DELETE FROM {table} WHERE {where_clause}"
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
            else:
                query = f"DELETE FROM {table}"
                cursor.execute(query)
            
            deleted = cursor.rowcount
            
            if commit:
                conn.commit()
                self.logger.info(f"Deleted {deleted} records from {table}")
            
            return deleted
            
        except sqlite3.Error as e:
            self.logger.error(f"Delete error in {table}: {e}")
            conn.rollback()
            raise
        finally:
            if commit:
                conn.close()
    
    def get_table_count(self, table: str) -> int:
        """Get count of records in a table"""
        try:
            result = self.fetch_one(f"SELECT COUNT(*) as count FROM {table}")
            return result['count'] if result else 0
        except sqlite3.Error as e:
            if "no such table" in str(e):
                self.logger.warning(f"Table '{table}' does not exist")
                return 0
            raise
    
    def table_exists(self, table: str) -> bool:
        """Check if a table exists"""
        result = self.fetch_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        exists = result is not None
        if not exists:
            self.logger.warning(f"Table '{table}' does not exist!")
        return exists
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get list of column names for a table"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            return columns
        except sqlite3.Error as e:
            self.logger.error(f"Error getting columns for {table}: {e}")
            return []
        finally:
            conn.close()
    
    def vacuum(self):
        """Vacuum database to reclaim space"""
        self.logger.info("Running VACUUM on database...")
        conn = self.get_connection()
        try:
            conn.execute("VACUUM")
            self.logger.info("✅ VACUUM completed")
        except sqlite3.Error as e:
            self.logger.error(f"VACUUM failed: {e}")
            raise
        finally:
            conn.close()
    
    def analyze(self):
        """Analyze database for query optimization"""
        self.logger.info("Running ANALYZE on database...")
        conn = self.get_connection()
        try:
            conn.execute("ANALYZE")
            self.logger.info("✅ ANALYZE completed")
        except sqlite3.Error as e:
            self.logger.error(f"ANALYZE failed: {e}")
            raise
        finally:
            conn.close()
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        stats = {}
        
        # Get all tables
        tables = self.fetch_all("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        
        # Get count for each table
        for table in tables:
            table_name = table['name']
            count = self.get_table_count(table_name)
            stats[table_name] = count
        
        # Get database file size
        db_path = Path(self.db_path)
        if db_path.exists():
            stats['database_size_mb'] = db_path.stat().st_size / (1024 * 1024)
        
        return stats
    
    def __del__(self):
        """Cleanup on deletion"""
        # SQLite connections are automatically closed, but we can be explicit
        pass


    def remove_closed_events(self):
        """Remove all closed/inactive events and their associated data"""
        self.logger.info("🧹 Removing closed/inactive events and associated data...")
        
        conn = self.get_connection()
        
        try:
            cursor = conn.cursor()
            
            # Get count before deletion
            cursor.execute("SELECT COUNT(*) FROM events WHERE closed = 1 OR active = 0")
            result = cursor.fetchone()
            closed_count = result[0] if result else 0
            
            if closed_count == 0:
                self.logger.info("No closed events to remove")
                return 0
            
            self.logger.info(f"Found {closed_count} closed/inactive events to remove")
            
            # Get IDs of closed events
            cursor.execute("SELECT id FROM events WHERE closed = 1 OR active = 0")
            closed_event_ids = [row[0] for row in cursor.fetchall()]
            
            if not closed_event_ids:
                return 0
            
            # Create placeholders for SQL IN clause
            placeholders = ','.join('?' * len(closed_event_ids))
            
            # Start transaction
            cursor.execute("BEGIN TRANSACTION")
            
            # Delete markets associated with closed events
            cursor.execute(f"SELECT COUNT(*) FROM markets WHERE event_id IN ({placeholders})", closed_event_ids)
            result = cursor.fetchone()
            markets_count = result[0] if result else 0
            
            cursor.execute(f"DELETE FROM markets WHERE event_id IN ({placeholders})", closed_event_ids)
            self.logger.info(f"  Deleted {markets_count} markets associated with closed events")
            
            # Delete comments associated with closed events
            cursor.execute(f"SELECT COUNT(*) FROM comments WHERE event_id IN ({placeholders})", closed_event_ids)
            result = cursor.fetchone()
            comments_count = result[0] if result else 0
            
            cursor.execute(f"DELETE FROM comments WHERE event_id IN ({placeholders})", closed_event_ids)
            self.logger.info(f"  Deleted {comments_count} comments associated with closed events")
            
            # Delete event_tags associated with closed events
            cursor.execute(f"SELECT COUNT(*) FROM event_tags WHERE event_id IN ({placeholders})", closed_event_ids)
            result = cursor.fetchone()
            event_tags_count = result[0] if result else 0
            
            cursor.execute(f"DELETE FROM event_tags WHERE event_id IN ({placeholders})", closed_event_ids)
            self.logger.info(f"  Deleted {event_tags_count} event_tags associated with closed events")
            
            # Delete the closed events themselves
            cursor.execute(f"DELETE FROM events WHERE id IN ({placeholders})", closed_event_ids)
            
            conn.commit()
            
            # Vacuum to reclaim space
            self.logger.info("  Running VACUUM to reclaim space...")
            conn.execute("VACUUM")
            
            self.logger.info(f"✅ Successfully removed {closed_count} closed events and associated data")
            
            return closed_count
            
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Remove closed events failed: {e}")
            raise
        finally:
            conn.close()