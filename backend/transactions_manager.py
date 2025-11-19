"""
Transactions Manager for Polymarket Terminal - MULTITHREADED
Handles fetching, processing, and storing transaction data with concurrent requests
"""

import requests
import time
import sqlite3
import gc
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from collections import defaultdict
from backend.database.database_manager import DatabaseManager
from backend.config import Config
from backend.fetch.entity.batch.batch_transactions import BatchTransactionsManager
from backend.fetch.entity.id.id_transactions import IdTransactionsManager
from backend.database.entity.store_transactions import StoreTransactionsManager

class TransactionsManager:
    """Manager for transaction-related operations with multithreading support"""
    
    def __init__(self):
        # Core configuration
        self.config = Config
        self.clob_api_url = Config.CLOB_API_URL if Config.CLOB_API_URL else "https://clob.polymarket.com"
        self.data_api_url = Config.DATA_API_URL if Config.DATA_API_URL else "https://data-api.polymarket.com"
        
        # Initialize managers
        self.db_manager = DatabaseManager()
        self.batch_manager = BatchTransactionsManager()
        self.id_manager = IdTransactionsManager()
        self.store_manager = StoreTransactionsManager()
        
        # Setup logging
        self.logger = self.db_manager.logger
        
        # Thread safety
        self._lock = Lock()
        
        # Whale thresholds
        self.MIN_TRANSACTION_SIZE = Config.MIN_TRANSACTION_SIZE if hasattr(Config, 'MIN_TRANSACTION_SIZE') else 500
        self.MIN_WHALE_TRADE = Config.MIN_WHALE_TRADE if hasattr(Config, 'MIN_WHALE_TRADE') else 10000
        self.MIN_POSITION_VALUE = 500

    def fetch_comprehensive_whale_data(self, limit_users: int = None) -> Dict[str, int]:
        """
        Main entry point for fetching comprehensive whale transaction and trading data
        This includes: transactions, positions, activity, portfolio values, and closed positions
        """
        self.logger.info("=" * 60)
        self.logger.info("🐋 STARTING COMPREHENSIVE WHALE DATA FETCH")
        self.logger.info("=" * 60)
        
        results = {
            'whale_users': 0,
            'transactions': 0,
            'current_positions': 0,
            'closed_positions': 0,
            'user_activities': 0,
            'portfolio_values': 0,
            'trades': 0
        }
        
        # Step 1: Get whale users from various sources
        whale_users = self._get_whale_users(limit_users)
        results['whale_users'] = len(whale_users)
        
        if not whale_users:
            self.logger.warning("No whale users found to process")
            return results
        
        self.logger.info(f"Processing {len(whale_users)} whale users...")
        
        # Step 2: Fetch current positions for whales
        self.logger.info("\n📊 Phase 1: Fetching Current Positions...")
        from backend.positions_manager import PositionsManager
        positions_mgr = PositionsManager()
        positions_result = positions_mgr.fetch_user_positions_batch(whale_users)
        results['current_positions'] = positions_result['total_positions']
        
        # Step 3: Fetch user trading activity
        self.logger.info("\n📈 Phase 2: Fetching User Activity...")
        activity_result = self.batch_manager.fetch_user_activity_batch(whale_users)
        results['user_activities'] = activity_result['total_activities']
        
        # Step 4: Fetch portfolio values
        self.logger.info("\n💰 Phase 3: Fetching Portfolio Values...")
        values_result = self.batch_manager.fetch_user_values_batch(whale_users)
        results['portfolio_values'] = values_result['values_fetched']
        
        # Step 5: Fetch closed positions
        self.logger.info("\n💸 Phase 4: Fetching Closed Positions...")
        closed_result = positions_mgr.fetch_closed_positions_batch(whale_users)
        results['closed_positions'] = closed_result['total_positions']
        
        # Step 6: Fetch trades for whales and top markets
        self.logger.info("\n📈 Phase 5: Fetching Whale Trades...")
        trades_result = self.batch_manager.fetch_user_trades_batch(whale_users[:200])
        results['trades'] = trades_result['total_trades']
        
        # Step 7: Fetch whale transactions from recent activity
        self.logger.info("\n💸 Phase 6: Fetching Recent Whale Transactions...")
        tx_result = self.fetch_recent_whale_transactions()
        results['transactions'] = tx_result
        
        # Print summary
        self.logger.info("\n" + "=" * 60)
        self.logger.info("✅ COMPREHENSIVE WHALE DATA FETCH COMPLETE!")
        self.logger.info("=" * 60)
        for key, value in results.items():
            self.logger.info(f"   {key:<20} {value:>10,}")
        
        return results

    def fetch_recent_whale_transactions(self) -> int:
        """Fetch recent whale transactions from CLOB API"""
        return self.batch_manager.fetch_recent_whale_transactions()

    def _get_whale_users(self, limit: int = None) -> List[str]:
        """Get unique whale users from various sources"""
        query = """
            SELECT DISTINCT proxy_wallet
            FROM (
                -- Users marked as whales
                SELECT proxy_wallet FROM users WHERE is_whale = 1
                UNION
                -- Users with large transactions
                SELECT DISTINCT proxy_wallet FROM transactions WHERE usdc_size >= ?
                UNION
                -- Users with large positions in market_holders
                SELECT DISTINCT proxy_wallet FROM market_holders WHERE amount >= ?
                UNION
                -- Users with significant trading activity
                SELECT DISTINCT proxy_wallet FROM user_trades WHERE size >= ?
            )
            ORDER BY proxy_wallet
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        users = self.db_manager.fetch_all(query, (
            self.MIN_TRANSACTION_SIZE,
            self.MIN_TRANSACTION_SIZE / 2,
            self.MIN_TRANSACTION_SIZE / 2
        ))
        
        return [u['proxy_wallet'] for u in users]

    def _close_all_connections(self):
        """Close all database connections from managers"""
        self.logger.info("Closing all transaction manager database connections...")
        
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

    def delete_transactions_only(self) -> Dict:
        """
        Delete transactions data
        
        Returns:
            Dict with success status, number deleted, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("🗑️  Deleting TRANSACTIONS & TRADING Data")
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
                    'transactions',
                    'user_activity', 
                    'user_trades',
                    'user_positions_current',
                    'user_positions_closed',
                    'user_values'
                ]
                
                counts = {}
                for table in tables_to_clear:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    counts[table] = cursor.fetchone()[0]
                
                # Begin exclusive transaction
                cursor.execute("BEGIN EXCLUSIVE")
                
                # Delete all transaction-related data
                for table in tables_to_clear:
                    self.logger.info(f"  Deleting from {table}: {counts[table]} records")
                    cursor.execute(f"DELETE FROM {table}")
                
                # Commit the transaction
                conn.commit()
                
                # Calculate total deleted
                total_deleted = sum(counts.values())
                
                result['deleted'] = total_deleted
                result['success'] = True
                
                self.logger.info(f"\n✅ Deleted transaction and trading data:")
                for table, count in counts.items():
                    if count > 0:
                        self.logger.info(f"   {table}: {count:,}")
                self.logger.info(f"   Total deleted: {total_deleted:,}")
                
            finally:
                conn.close()
                
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                result['error'] = "Database is locked. Please ensure no other processes are accessing the database."
                self.logger.error(f"❌ Database is locked. Try closing any other programs accessing the database.")
            else:
                result['error'] = str(e)
                self.logger.error(f"❌ Error deleting transactions: {e}")
        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"❌ Error deleting transactions: {e}")
        
        finally:
            # Reinitialize connections for future operations
            self.db_manager = DatabaseManager()
            
        return result

    def load_transactions_only(self, comprehensive: bool = True) -> Dict:
        """
        Load only transactions data
        
        Args:
            comprehensive: If True, fetch comprehensive whale data
        
        Returns:
            Dict with success status, counts, and any error
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("💰 Loading TRANSACTIONS & WHALE DATA")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        
        if comprehensive:
            # Use the comprehensive whale data fetching
            self.logger.info("Using comprehensive whale data fetching...")
            self.logger.info("This will fetch:")
            self.logger.info("  • Whale transactions")
            self.logger.info("  • Current positions")
            self.logger.info("  • User activity")
            self.logger.info("  • Portfolio values")
            self.logger.info("  • Closed positions (>$500)")
            self.logger.info("  • User trades")
            
            try:
                result = self.fetch_comprehensive_whale_data()
                result['success'] = True
                
                elapsed_time = time.time() - start_time
                self.logger.info(f"⏱️  Time taken: {elapsed_time/60:.2f} minutes")
                
                return result
                
            except Exception as e:
                self.logger.error(f"❌ Error in comprehensive whale data fetch: {e}")
                return {'success': False, 'error': str(e)}
        
        else:
            # Legacy method - just fetch basic transactions
            self.logger.info("Using legacy transaction fetching...")
            result = {'success': False, 'transactions': 0, 'error': None}
            
            try:
                # Fetch recent whale transactions
                txns = self.fetch_recent_whale_transactions()
                result['transactions'] = txns
                result['success'] = True
                
                elapsed_time = time.time() - start_time
                self.logger.info(f"✅ Transactions loaded: {result['transactions']}")
                self.logger.info(f"⏱️  Time taken: {elapsed_time:.2f} seconds")
                
            except Exception as e:
                result['error'] = str(e)
                self.logger.error(f"❌ Error loading transactions: {e}")
            
            return result