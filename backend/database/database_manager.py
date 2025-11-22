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
        
        # Initialize database schema
        self.initialize_schema()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger for database operations"""
        logger = logging.getLogger('DatabaseManager')
        logger.setLevel(logging.WARNING)  # Only warnings and errors
        
        # Console handler
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.WARNING)
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
        pass
    
    def initialize_schema(self):
        """Initialize database schema from database_schema.py"""
        try:
            # Import the schema
            from backend.database.database_schema import get_schema
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Get the full schema
            schema_sql = get_schema()
            
            # Split by semicolons to execute each statement separately
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            
            # Execute all statements
            errors = []
            for statement in statements:
                if statement.strip():
                    try:
                        cursor.execute(statement)
                    except sqlite3.Error as e:
                        # Only log actual errors, not "already exists" warnings
                        if "already exists" not in str(e).lower():
                            errors.append(str(e))
            
            conn.commit()
            conn.close()
            
            # Only log if there were actual errors
            if errors:
                self.logger.error(f"Schema initialization had {len(errors)} errors")
                for error in errors[:5]:  # Show first 5 errors only
                    self.logger.error(f"  {error}")
            
        except ImportError as e:
            self.logger.error(f"Failed to import schema: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize schema: {e}")
            raise
    
    def verify_tables(self):
        """Verify all required tables exist - silent unless errors"""
        required_tables = [
            'events', 'markets', 'series', 'tags', 'users', 'comments'
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

            # Check for missing tables
            missing_tables = []
            for table in required_tables:
                if table not in existing_tables:
                    missing_tables.append(table)

            if missing_tables:
                self.logger.error(f"Missing tables: {', '.join(missing_tables)}")
                # Try to create missing tables by re-initializing schema
                self.initialize_schema()

            conn.close()

        except Exception as e:
            self.logger.error(f"Error verifying tables: {e}")

    def drop_table(self, table_name: str) -> bool:
        """Drop a single table from the database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
            conn.close()
            self.logger.info(f"✅ Dropped table: {table_name}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error dropping table {table_name}: {e}")
            return False

    def drop_all_tables(self) -> dict:
        """Drop all tables from the database"""
        result = {'success': False, 'dropped': [], 'errors': []}

        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get all table names
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]

            # Disable foreign keys temporarily to avoid constraint issues
            cursor.execute("PRAGMA foreign_keys = OFF")

            # Drop each table
            for table in tables:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    result['dropped'].append(table)
                except Exception as e:
                    result['errors'].append(f"{table}: {e}")

            conn.commit()

            # Re-enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.close()

            result['success'] = len(result['errors']) == 0
            self.logger.info(f"✅ Dropped {len(result['dropped'])} tables")

        except Exception as e:
            self.logger.error(f"❌ Error dropping all tables: {e}")
            result['errors'].append(str(e))

        return result

    def reset_database(self) -> dict:
        """Drop all tables and reinitialize the schema"""
        result = {'success': False, 'dropped': 0, 'created': 0, 'error': None}

        try:
            self.logger.info("🔄 Resetting database...")

            # Drop all tables
            drop_result = self.drop_all_tables()
            result['dropped'] = len(drop_result['dropped'])

            if drop_result['errors']:
                self.logger.warning(f"Some errors during drop: {drop_result['errors']}")

            # Reinitialize schema
            self.logger.info("📦 Reinitializing schema...")
            self.initialize_schema()

            # Count tables created
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            result['created'] = cursor.fetchone()[0]
            conn.close()

            result['success'] = True
            self.logger.info(f"✅ Database reset complete: {result['dropped']} dropped, {result['created']} created")

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error resetting database: {e}")

        return result

    def reset_table(self, table_name: str) -> dict:
        """Drop and recreate a single table"""
        result = {'success': False, 'table': table_name, 'error': None}

        try:
            self.logger.info(f"🔄 Resetting table: {table_name}")

            # Drop the table
            if not self.drop_table(table_name):
                result['error'] = f"Failed to drop table {table_name}"
                return result

            # Reinitialize schema (will recreate the dropped table)
            self.initialize_schema()

            # Verify table was recreated
            if self.table_exists(table_name):
                result['success'] = True
                self.logger.info(f"✅ Table {table_name} reset successfully")
            else:
                result['error'] = f"Table {table_name} was not recreated"

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error resetting table {table_name}: {e}")

        return result

    def reset_tables(self, table_names: list) -> dict:
        """Drop and recreate multiple tables"""
        result = {'success': False, 'reset': [], 'errors': []}

        try:
            self.logger.info(f"🔄 Resetting {len(table_names)} tables...")

            conn = self.get_connection()
            cursor = conn.cursor()

            # Disable foreign keys temporarily
            cursor.execute("PRAGMA foreign_keys = OFF")

            # Drop each table
            for table in table_names:
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    self.logger.info(f"  Dropped: {table}")
                except Exception as e:
                    result['errors'].append(f"{table}: {e}")

            conn.commit()
            cursor.execute("PRAGMA foreign_keys = ON")
            conn.close()

            # Reinitialize schema to recreate dropped tables
            self.initialize_schema()

            # Verify tables were recreated
            for table in table_names:
                if self.table_exists(table):
                    result['reset'].append(table)
                else:
                    result['errors'].append(f"{table}: not recreated")

            result['success'] = len(result['errors']) == 0
            self.logger.info(f"✅ Reset {len(result['reset'])} tables")

        except Exception as e:
            self.logger.error(f"❌ Error resetting tables: {e}")
            result['errors'].append(str(e))

        return result
    
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
            return dict(row) if row else None
        except sqlite3.Error as e:
            self.logger.error(f"Fetch error: {e}")
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
            self.logger.error(f"Fetch all error: {e}")
            raise
        finally:
            conn.close()
    
    def insert(self, table: str, data: Dict) -> int:
        """Insert single record"""
        if not data:
            return 0
        
        columns = list(data.keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        values = [data[col] for col in columns]
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        try:
            return self.execute(query, tuple(values))
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint" in str(e):
                # Silently skip duplicates
                return 0
            raise
    
    def insert_or_replace(self, table: str, data: Dict) -> int:
        """Insert or replace single record"""
        if not data:
            return 0
        
        columns = list(data.keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        values = [data[col] for col in columns]
        
        query = f"INSERT OR REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, tuple(values))
    
    def insert_or_ignore(self, table: str, data: Dict) -> int:
        """Insert single record, ignore if exists"""
        if not data:
            return 0
        
        columns = list(data.keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        values = [data[col] for col in columns]
        
        query = f"INSERT OR IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})"
        return self.execute(query, tuple(values))
    
    def update(self, table: str, data: Dict, where_clause: str, params: tuple = None) -> int:
        """Update records"""
        if not data:
            return 0
        
        set_clause = ','.join([f"{col} = ?" for col in data.keys()])
        values = list(data.values())
        
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        
        if params:
            values.extend(params)
        
        return self.execute(query, tuple(values))
    
    def bulk_insert(self, table: str, data: List[Dict]) -> int:
        """Bulk insert records"""
        if not data:
            return 0
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        # Convert to list of tuples
        params_list = []
        for record in data:
            values = [record.get(col) for col in columns]
            params_list.append(tuple(values))
        
        return self.executemany(query, params_list)
    
    def bulk_insert_or_replace(self, table: str, data: List[Dict], batch_size: int = 1000) -> int:
        """Bulk insert or replace records in batches"""
        if not data:
            return 0
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        
        query = f"INSERT OR REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            total_inserted = 0
            
            # Process in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                params_list = []
                for record in batch:
                    values = [record.get(col) for col in columns]
                    params_list.append(tuple(values))
                
                cursor.executemany(query, params_list)
                total_inserted += cursor.rowcount
                conn.commit()
            
            return total_inserted
            
        except sqlite3.Error as e:
            self.logger.error(f"Bulk insert error in {table}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def bulk_insert_or_ignore(self, table: str, data: List[Dict], batch_size: int = 1000) -> int:
        """Bulk insert records, ignoring duplicates, in batches"""
        if not data:
            return 0
        
        # Get columns from first record
        columns = list(data[0].keys())
        placeholders = ','.join(['?' for _ in columns])
        columns_str = ','.join(columns)
        
        query = f"INSERT OR IGNORE INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            total_inserted = 0
            
            # Process in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                params_list = []
                for record in batch:
                    values = [record.get(col) for col in columns]
                    params_list.append(tuple(values))
                
                cursor.executemany(query, params_list)
                total_inserted += cursor.rowcount
                conn.commit()
            
            return total_inserted
            
        except sqlite3.Error as e:
            self.logger.error(f"Bulk insert error in {table}: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def delete_records(self, table: str, where_clause: str = None, params: tuple = None, commit: bool = True) -> int:
        """Delete records from a table"""
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
                return 0
            raise
    
    def table_exists(self, table: str) -> bool:
        """Check if a table exists"""
        result = self.fetch_one(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        return result is not None
    
    def get_table_columns(self, table: str) -> List[str]:
        """Get list of column names for a table"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [row[1] for row in cursor.fetchall()]
            return columns
        finally:
            conn.close()
    
    def vacuum(self):
        """Vacuum database to reclaim space"""
        conn = self.get_connection()
        try:
            conn.execute("VACUUM")
            conn.commit()
        finally:
            conn.close()
    
    def get_database_size(self) -> float:
        """Get database file size in MB"""
        db_path = Path(self.db_path)
        if db_path.exists():
            size_bytes = db_path.stat().st_size
            return size_bytes / (1024 * 1024)  # Convert to MB
        return 0.0
    
    def backup_database(self, backup_path: str = None):
        """Create a backup of the database"""
        import shutil
        
        if not backup_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"{self.db_path}.backup_{timestamp}"
        
        try:
            shutil.copy2(self.db_path, backup_path)
            self.logger.info(f"Database backed up to: {backup_path}")
            return backup_path
        except Exception as e:
            self.logger.error(f"Backup failed: {e}")
            raise
    
    def optimize_database(self):
        """Optimize database performance"""
        conn = self.get_connection()
        try:
            conn.execute("ANALYZE")
            conn.execute("REINDEX")
            conn.commit()
        finally:
            conn.close()
    
    def get_table_stats(self) -> Dict[str, int]:
        """Get record count for all tables"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
            """)
            
            stats = {}
            for row in cursor.fetchall():
                table_name = row[0]
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                stats[table_name] = count_result[0] if count_result else 0
            
            return stats
        finally:
            conn.close()
    
    def remove_closed_events(self) -> int:
        """Remove all closed events from the database"""
        try:
            # First delete related records
            self.delete_records('event_tags', 'event_id IN (SELECT id FROM events WHERE closed = 1)')
            self.delete_records('event_live_volume', 'event_id IN (SELECT id FROM events WHERE closed = 1)')
            self.delete_records('comments', 'event_id IN (SELECT id FROM events WHERE closed = 1)')
            self.delete_records('markets', 'event_id IN (SELECT id FROM events WHERE closed = 1)')
            
            # Then delete the closed events
            deleted = self.delete_records('events', 'closed = 1')
            
            return deleted
            
        except Exception as e:
            self.logger.error(f"Error removing closed events: {e}")
            raise
    
    def clear_all_data(self):
        """Clear all data from all tables (keeping schema)"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            
            # Disable foreign keys temporarily
            cursor.execute("PRAGMA foreign_keys = OFF")
            
            # Get all tables
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            # Clear each table
            for table in tables:
                cursor.execute(f"DELETE FROM {table}")
            
            # Re-enable foreign keys
            cursor.execute("PRAGMA foreign_keys = ON")
            
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()