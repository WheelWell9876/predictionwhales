"""
Base Database Manager for Polymarket Terminal
Handles common database operations and connection management
"""

import sqlite3
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager
from .config import Config

class DatabaseManager:
    """Base class for database operations"""
    
    def __init__(self, db_path: str = None):
        from .config import Config
        self.config = Config
        self.db_path = db_path or Config.DATABASE_PATH
        self.logger = self._setup_logger()
        
    def _setup_logger(self):
        """Setup logger for database operations"""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        
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
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = None, commit: bool = False):
        """Execute a single query"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if commit:
                conn.commit()
            
            return cursor
    
    def execute_many(self, query: str, params_list: List[tuple], commit: bool = True):
        """Execute many queries with different parameters"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            
            if commit:
                conn.commit()
            
            return cursor
    
    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Fetch single row as dictionary"""
        cursor = self.execute_query(query, params)
        row = cursor.fetchone()
        return dict(row) if row else None
    
    def fetch_all(self, query: str, params: tuple = None) -> List[Dict]:
        """Fetch all rows as list of dictionaries"""
        cursor = self.execute_query(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def insert_or_replace(self, table: str, data: Dict, commit: bool = True):
        """Insert or replace a single record"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(data.values()))
            
            if commit:
                conn.commit()
            
            return cursor.lastrowid
    
    def insert_or_ignore(self, table: str, data: Dict, commit: bool = True):
        """Insert or ignore a single record"""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['?' for _ in data])
        query = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, tuple(data.values()))
            
            if commit:
                conn.commit()
            
            return cursor.lastrowid
    
    def bulk_insert_or_replace(self, table: str, data_list: List[Dict], 
                              batch_size: int = None, commit_interval: int = None):
        """Bulk insert or replace records"""
        if not data_list:
            return
        
        batch_size = batch_size or self.config.BATCH_SIZE
        commit_interval = commit_interval or self.config.COMMIT_INTERVAL
        
        # Get columns from first record
        columns = list(data_list[0].keys())
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?' for _ in columns])
        query = f"INSERT OR REPLACE INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                params_list = [tuple(record.get(col) for col in columns) for record in batch]
                
                cursor.executemany(query, params_list)
                
                # Commit at intervals
                if (i + batch_size) % commit_interval == 0:
                    conn.commit()
                    self.logger.debug(f"Committed {i + batch_size} records to {table}")
            
            conn.commit()
            self.logger.info(f"Bulk inserted {len(data_list)} records into {table}")
    
    def update_record(self, table: str, data: Dict, where_clause: str, 
                     where_params: tuple = None, commit: bool = True):
        """Update records in table"""
        set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        
        params = tuple(data.values())
        if where_params:
            params = params + where_params
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            
            if commit:
                conn.commit()
            
            return cursor.rowcount
    
    def delete_records(self, table: str, where_clause: str = None, 
                       where_params: tuple = None, commit: bool = True):
        """Delete records from table"""
        query = f"DELETE FROM {table}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            if where_params:
                cursor.execute(query, where_params)
            else:
                cursor.execute(query)
            
            if commit:
                conn.commit()
            
            return cursor.rowcount
    
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
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.fetch_one(query)
        return result['count'] if result else 0
    
    def reset_table(self, table_name: str):
        """Clear all records from a table"""
        self.delete_records(table_name, commit=True)
        self.logger.info(f"Reset table: {table_name}")
    
    def reset_database(self):
        """Reset entire database (clear all data but keep schema)"""
        tables = [
            'events', 'markets', 'tags', 'series', 'collections',
            'event_tags', 'market_tags', 'series_events', 'series_collections',
            'series_tags', 'collection_tags', 'tag_relationships',
            'event_live_volume', 'market_open_interest',
            'users', 'user_positions_current', 'user_positions_closed',
            'user_trades', 'user_values', 'market_holders',
            'comments', 'comment_reactions', 'user_activity'
        ]
        
        with self.get_connection() as conn:
            for table in tables:
                if self.table_exists(table):
                    conn.execute(f"DELETE FROM {table}")
                    self.logger.info(f"Cleared table: {table}")
            
            conn.commit()
            conn.execute("VACUUM")  # Reclaim space
            
        self.logger.info("Database reset complete")
    
    def get_statistics(self) -> Dict[str, int]:
        """Get database statistics"""
        stats = {}
        tables = [
            'events', 'markets', 'tags', 'series', 'collections',
            'event_tags', 'market_tags', 'users'
        ]
        
        for table in tables:
            if self.table_exists(table):
                stats[table] = self.get_table_count(table)
        
        return stats
    
    def backup_database(self, backup_path: str = None):
        """Create a backup of the database"""
        import shutil
        from datetime import datetime
        
        if not backup_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"{self.db_path}.backup_{timestamp}"
        
        shutil.copy2(self.db_path, backup_path)
        self.logger.info(f"Database backed up to: {backup_path}")
        return backup_path