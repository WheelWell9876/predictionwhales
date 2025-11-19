"""
Positions Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing position data with concurrent requests
"""

import requests
import time
import sqlite3
import gc
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.fetch.entity.batch.batch_positions import BatchPositionsManager
from backend.fetch.entity.id.id_positions import IdPositionsManager
from backend.database.entity.store_positions import StorePositionsManager

class PositionsManager:
    """Manager for position-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchPositionsManager()
        self.id_manager = IdPositionsManager()
        self.store_manager = StorePositionsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
        
        # Position thresholds
        self.MIN_POSITION_VALUE = 500  # Minimum position value to track

    def fetch_user_positions_batch(self, users: List[str]) -> Dict[str, int]:
        """
        Fetch current positions for a batch of users
        """
        return self.batch_manager.fetch_user_positions_batch(users)

    def fetch_closed_positions_batch(self, users: List[str]) -> Dict[str, int]:
        """
        Fetch closed positions for a batch of users
        """
        return self.batch_manager.fetch_closed_positions_batch(users)

    def fetch_user_current_positions(self, proxy_wallet: str) -> List[Dict]:
        """
        Fetch current positions for a single user
        """
        return self.id_manager.fetch_user_current_positions(proxy_wallet)

    def fetch_user_closed_positions(self, proxy_wallet: str) -> List[Dict]:
        """
        Fetch closed positions for a single user
        """
        return self.id_manager.fetch_user_closed_positions(proxy_wallet)

    def fetch_all_whale_positions(self, limit_users: int = None) -> Dict[str, int]:
        """
        Fetch positions for all whale users
        """
        self.logger.info("=" * 60)
        self.logger.info("📊 FETCHING WHALE POSITIONS")
        self.logger.info("=" * 60)
        
        # Get whale users
        query = """
            SELECT DISTINCT proxy_wallet
            FROM users
            WHERE is_whale = 1
            ORDER BY total_value DESC
        """
        
        if limit_users:
            query += f" LIMIT {limit_users}"
        
        whale_users = self.db_manager.fetch_all(query)
        user_list = [u['proxy_wallet'] for u in whale_users]
        
        if not user_list:
            self.logger.warning("No whale users found")
            return {'current_positions': 0, 'closed_positions': 0}
        
        self.logger.info(f"Processing positions for {len(user_list)} whale users...")
        
        # Fetch current positions
        self.logger.info("\n📈 Fetching Current Positions...")
        current_result = self.fetch_user_positions_batch(user_list)
        
        # Fetch closed positions
        self.logger.info("\n💸 Fetching Closed Positions...")
        closed_result = self.fetch_closed_positions_batch(user_list)
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("✅ POSITION FETCH COMPLETE")
        self.logger.info(f"   Current positions: {current_result.get('total_positions', 0):,}")
        self.logger.info(f"   Closed positions: {closed_result.get('total_positions', 0):,}")
        
        return {
            'current_positions': current_result.get('total_positions', 0),
            'closed_positions': closed_result.get('total_positions', 0)
        }

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all position manager database connections...")
        
        # Close connections from all sub-managers
        managers = [
            self.db_manager,
            self.batch_manager,
            self.id_manager,
            self.store_manager
        ]
        
        for manager in managers:
            try:
                if hasattr(manager, 'close_connection'):
                    manager.close_connection()
            except:
                pass
        
        # Force garbage collection
        gc.collect()
        
        # Small delay to ensure connections are closed
        time.sleep(0.5)

    def delete_positions_only(self) -> Dict:
        """
        Delete positions data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting POSITIONS Data")
        self.logger.info("=" * 60)
        
        result = {'success': False, 'deleted': 0, 'error': None}
        
        try:
            # Close all connections first
            self._close_all_connections()
            
            # Create a fresh database connection for deletion
            conn = sqlite3.connect(
                self.db_manager.db_path,
                timeout=30.0,
                isolation_level='EXCLUSIVE'
            )
            
            try:
                cursor = conn.cursor()
                
                # Enable WAL mode
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA synchronous=NORMAL")
                
                # Get counts before deletion
                tables_to_clear = [
                    'user_positions_current',
                    'user_positions_closed'
                ]
                
                counts = {}
                for table in tables_to_clear:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all position data
                for table in tables_to_clear:
                    self.logger.info(f"  Deleting from {table}: {counts[table]} records")
                    cursor.execute(f"DELETE FROM {table}")
                
                # Commit the transaction
                conn.commit()
                
                # Calculate total deleted
                total_deleted = sum(counts.values())
                
                result['deleted'] = total_deleted
                result['success'] = True
                
                self.logger.info(f"✅ Deleted {total_deleted:,} position records")
                
            finally:
                conn.close()
                
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting positions: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_positions_only(self, whale_users_only: bool = True) -> Dict:
        """
        Load only positions data
        
        Args:
            whale_users_only: If True, only fetch positions for whale users
        
        Returns:
            Dict with success status, counts, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"📊 Loading {'WHALE' if whale_users_only else 'ALL'} POSITIONS")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'current': 0, 'closed': 0, 'error': None}
        
        try:
            if whale_users_only:
                positions = self.fetch_all_whale_positions()
                result['current'] = positions['current_positions']
                result['closed'] = positions['closed_positions']
            else:
                # Get all users with any activity
                all_users = self.db_manager.fetch_all("""
                    SELECT DISTINCT proxy_wallet
                    FROM users
                    ORDER BY total_value DESC
                    LIMIT 500
                """)
                
                user_list = [u['proxy_wallet'] for u in all_users]
                
                if user_list:
                    # Fetch current positions
                    current = self.fetch_user_positions_batch(user_list)
                    result['current'] = current.get('total_positions', 0)
                    
                    # Fetch closed positions
                    closed = self.fetch_closed_positions_batch(user_list)
                    result['closed'] = closed.get('total_positions', 0)
            
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Current positions loaded: {result['current']:,}")
            self.logger.info(f"✅ Closed positions loaded: {result['closed']:,}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading positions: {e}")
            
        return result