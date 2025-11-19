"""
Users Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing user data with concurrent requests
"""

import requests
import json
import time
import sqlite3
import gc
from datetime import datetime
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.fetch.entity.batch.batch_users import BatchUsersManager
from backend.fetch.entity.id.id_users import IdUsersManager
from backend.database.entity.store_users import StoreUsersManager

class UsersManager:
    """Manager for user-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.data_api_url = Config.DATA_API_URL
        self.base_url = Config.GAMMA_API_URL
        self.clob_url = Config.CLOB_API_URL
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchUsersManager()
        self.id_manager = IdUsersManager()
        self.store_manager = StoreUsersManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
        
        # Whale thresholds
        self.MIN_WALLET_VALUE = 1000  # $1000 minimum wallet value
        self.MIN_POSITION_VALUE = 250  # $250 minimum position value
        self.TOP_HOLDERS_PER_MARKET = 25  # Top 25 holders per market

    def fetch_top_holders_for_all_markets(self) -> Dict[str, int]:
        """
        Fetch top 25 holders for ALL active markets using multithreading
        Only store users meeting whale criteria ($1000+ wallet OR $250+ position)
        """
        return self.batch_manager.fetch_top_holders_for_all_markets()

    def fetch_top_holders_for_markets(self, limit_markets: int = 100) -> int:
        """Legacy method - calls new whale-focused method"""
        result = self.fetch_top_holders_for_all_markets()
        return result['total_whales_found']

    def enrich_all_whale_users(self) -> Dict[str, int]:
        """
        Enrich all whale users with complete profile data using multithreading
        """
        return self.id_manager.enrich_all_whale_users()

    def batch_enrich_whales(self, wallet_addresses: List[str]) -> Dict[str, int]:
        """
        Enrich multiple whale users in parallel
        """
        return self.id_manager.batch_enrich_whales(wallet_addresses)

    def identify_whale_users(self) -> List[str]:
        """Get list of all whale wallet addresses"""
        whales = self.db_manager.fetch_all("SELECT proxy_wallet FROM users WHERE is_whale = 1")
        return [w['proxy_wallet'] for w in whales]

    def fetch_user_activity_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch detailed activity for a batch of users"""
        return self.batch_manager.fetch_user_activity_batch(users)

    def fetch_user_values_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch portfolio values for a batch of users"""
        return self.batch_manager.fetch_user_values_batch(users)

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all user manager database connections...")
        
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

    def delete_users_only(self) -> Dict:
        """
        Delete users data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting USERS Data")
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
                
                # Get current count
                cursor.execute("SELECT COUNT(*) FROM users")
                before_count = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete users table (cascades should handle related tables)
                cursor.execute("DELETE FROM users")
                self.logger.info(f"  Cleared table: users")
                
                # Commit the transaction
                conn.commit()
                
                result['deleted'] = before_count
                
            finally:
                conn.close()
            
            result['success'] = True
            self.logger.info(f"✅ Deleted {result['deleted']} users")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting users: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_users_only(self, whales_only: bool = True) -> Dict:
        """
        Load only users data
        
        Args:
            whales_only: If True, only fetch whale users
        
        Returns:
            Dict with success status, counts, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"👥 Loading {'WHALE' if whales_only else 'ALL'} USERS Only")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        result = {'success': False, 'whales_found': 0, 'enriched': 0, 'error': None}
        
        try:
            # Fetch top holders from all markets
            self.logger.info("🔍 Fetching top holders from all markets...")
            holders_result = self.fetch_top_holders_for_all_markets()
            result['whales_found'] = holders_result['total_whales_found']
            
            # Enrich whale profiles
            if result['whales_found'] > 0:
                self.logger.info("📈 Enriching whale profiles...")
                enrich_result = self.enrich_all_whale_users()
                result['enriched'] = enrich_result['total_whales_enriched']
            
            result['success'] = True
            
            elapsed_time = time.time() - start_time
            self.logger.info(f"✅ Whales found: {result['whales_found']}")
            self.logger.info(f"✅ Profiles enriched: {result['enriched']}")
            self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
            
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error loading users: {e}")
            
        return result