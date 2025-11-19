#!/usr/bin/env python3
"""
Database Utilities for Polymarket Terminal
Force close connections and perform clean operations
Database is located in project root, not in backend directory
"""

import sqlite3
import sys
import os
import time
from pathlib import Path

def get_db_path():
    """Get the database path - always in project root"""
    # Get project root (parent of backend directory)
    current_file = Path(__file__)
    backend_dir = current_file.parent.parent  # backend directory
    project_root = backend_dir.parent  # project root
    return project_root / 'polymarket_terminal.db'

def force_close_database_connections(db_path=None):
    """Force close all connections to the database"""
    if db_path is None:
        db_path = get_db_path()
    
    print(f"🔒 Force closing all connections to {db_path}...")
    
    # Method 1: Try to find and kill processes using the database (if psutil available)
    try:
        import psutil
        current_pid = os.getpid()
        
        for proc in psutil.process_iter(['pid', 'name', 'open_files']):
            try:
                # Check if this process has the database file open
                if proc.info['open_files']:
                    for file in proc.info['open_files']:
                        if str(db_path) in file.path and proc.info['pid'] != current_pid:
                            print(f"  Found process {proc.info['pid']} using database")
                            proc.terminate()
                            print(f"  Terminated process {proc.info['pid']}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except ImportError:
        print("  psutil not available, skipping process check")
    except Exception as e:
        print(f"  Warning: Could not check processes: {e}")
    
    # Method 2: Try to close WAL and SHM files
    wal_file = Path(str(db_path) + "-wal")
    shm_file = Path(str(db_path) + "-shm")
    
    if wal_file.exists():
        print(f"  Removing WAL file: {wal_file}")
        try:
            wal_file.unlink()
        except:
            print("    Could not remove WAL file")
    
    if shm_file.exists():
        print(f"  Removing SHM file: {shm_file}")
        try:
            shm_file.unlink()
        except:
            print("    Could not remove SHM file")
    
    # Small delay to ensure cleanup
    time.sleep(1)
    print("✅ Connection cleanup complete")

def delete_transaction_data(db_path=None):
    """Delete all transaction and trading data with exclusive access"""
    if db_path is None:
        db_path = get_db_path()
    
    # First force close any existing connections
    force_close_database_connections(db_path)
    
    print(f"\n🗑️ Deleting transaction and trading data from {db_path}...")
    
    try:
        # Connect with exclusive lock
        conn = sqlite3.connect(
            str(db_path),
            timeout=30.0,
            isolation_level='EXCLUSIVE'
        )
        
        cursor = conn.cursor()
        
        # Set pragmas for exclusive access
        cursor.execute("PRAGMA journal_mode=DELETE")  # Switch from WAL to DELETE mode
        cursor.execute("PRAGMA synchronous=FULL")
        
        # Tables to clear
        tables_to_clear = [
            'transactions',
            'user_activity', 
            'user_trades',
            'user_positions_current',
            'user_positions_closed',
            'user_values'
        ]
        
        # Begin exclusive transaction
        cursor.execute("BEGIN EXCLUSIVE")
        
        total_deleted = 0
        for table in tables_to_clear:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    cursor.execute(f"DELETE FROM {table}")
                    print(f"  ✅ Deleted {count:,} records from {table}")
                    total_deleted += count
                else:
                    print(f"  ⚪ {table} is already empty")
                    
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    print(f"  ⚠️ Table {table} doesn't exist")
                else:
                    raise
        
        # Commit the transaction
        conn.commit()
        
        # Vacuum to reclaim space
        print("\n🧹 Running VACUUM to reclaim space...")
        cursor.execute("VACUUM")
        
        # Switch back to WAL mode for better performance
        cursor.execute("PRAGMA journal_mode=WAL")
        
        conn.close()
        
        print(f"\n✅ Successfully deleted {total_deleted:,} records from transaction tables")
        return True
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e):
            print(f"\n❌ Database is still locked: {e}")
            print("\n💡 Try these steps:")
            print("  1. Close any Python scripts accessing the database")
            print("  2. Close any database browsers (like DB Browser for SQLite)")
            print("  3. Restart your terminal")
            print("  4. Run this script again")
        else:
            print(f"\n❌ Database error: {e}")
        return False
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False

def delete_positions_data(db_path=None):
    """Delete all positions data with exclusive access"""
    if db_path is None:
        db_path = get_db_path()
    
    # First force close any existing connections
    force_close_database_connections(db_path)
    
    print(f"\n🗑️ Deleting positions data from {db_path}...")
    
    try:
        # Connect with exclusive lock
        conn = sqlite3.connect(
            str(db_path),
            timeout=30.0,
            isolation_level='EXCLUSIVE'
        )
        
        cursor = conn.cursor()
        
        # Set pragmas for exclusive access
        cursor.execute("PRAGMA journal_mode=DELETE")
        cursor.execute("PRAGMA synchronous=FULL")
        
        # Tables to clear
        tables_to_clear = [
            'user_positions_current',
            'user_positions_closed'
        ]
        
        # Begin exclusive transaction
        cursor.execute("BEGIN EXCLUSIVE")
        
        total_deleted = 0
        for table in tables_to_clear:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                if count > 0:
                    cursor.execute(f"DELETE FROM {table}")
                    print(f"  ✅ Deleted {count:,} records from {table}")
                    total_deleted += count
                else:
                    print(f"  ⚪ {table} is already empty")
                    
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    print(f"  ⚠️ Table {table} doesn't exist")
                else:
                    raise
        
        # Commit the transaction
        conn.commit()
        
        # Vacuum to reclaim space
        print("\n🧹 Running VACUUM to reclaim space...")
        cursor.execute("VACUUM")
        
        # Switch back to WAL mode
        cursor.execute("PRAGMA journal_mode=WAL")
        
        conn.close()
        
        print(f"\n✅ Successfully deleted {total_deleted:,} records from positions tables")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

def check_database_status(db_path=None):
    """Check the status of the database and tables"""
    if db_path is None:
        db_path = get_db_path()
    
    print(f"\n📊 Checking database status: {db_path}")
    
    if not db_path.exists():
        print("  ❌ Database does not exist!")
        return
    
    # Get file size
    size_mb = db_path.stat().st_size / (1024 * 1024)
    print(f"  💾 Database size: {size_mb:.2f} MB")
    
    try:
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        cursor = conn.cursor()
        
        # Check journal mode
        cursor.execute("PRAGMA journal_mode")
        mode = cursor.fetchone()[0]
        print(f"  Journal mode: {mode}")
        
        # Check if database is locked
        try:
            cursor.execute("BEGIN EXCLUSIVE")
            cursor.execute("ROLLBACK")
            print("  Database lock: Available ✅")
        except:
            print("  Database lock: LOCKED ❌")
        
        # Check all tables
        sections = [
            ('events', 'Events'),
            ('markets', 'Markets'),
            ('series', 'Series'),
            ('tags', 'Tags'),
            ('users', 'Users'),
            ('comments', 'Comments'),
            ('transactions', 'Transactions'),
            ('user_activity', 'User Activity'),
            ('user_trades', 'User Trades'),
            ('user_positions_current', 'Current Positions'),
            ('user_positions_closed', 'Closed Positions'),
            ('user_values', 'User Values')
        ]
        
        print("\n  Table record counts:")
        total_records = 0
        for table, name in sections:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count
                print(f"    {name:<25} {count:>10,} records")
            except:
                print(f"    {name:<25} {'(not found)':>10}")
        
        print(f"    {'TOTAL':<25} {total_records:>10,} records")
        
        # Check for whale users
        try:
            cursor.execute("SELECT COUNT(*) FROM users WHERE is_whale = 1")
            whale_count = cursor.fetchone()[0]
            print(f"\n  🐋 Whale users: {whale_count:,}")
        except:
            pass
        
        # Check for active vs closed events
        try:
            cursor.execute("SELECT COUNT(*) FROM events WHERE closed = 0")
            active = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM events WHERE closed = 1")
            closed = cursor.fetchone()[0]
            print(f"\n  📊 Events:")
            print(f"    Active: {active:,}")
            print(f"    Closed: {closed:,}")
        except:
            pass
        
        conn.close()
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e):
            print("  ❌ Database is LOCKED - another process is using it")
        else:
            print(f"  ❌ Error accessing database: {e}")
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")

def optimize_database(db_path=None):
    """Optimize the database"""
    if db_path is None:
        db_path = get_db_path()
    
    print(f"\n🔧 Optimizing database: {db_path}")
    
    try:
        conn = sqlite3.connect(str(db_path), timeout=30.0)
        cursor = conn.cursor()
        
        # Run VACUUM
        print("  Running VACUUM...")
        cursor.execute("VACUUM")
        
        # Run ANALYZE
        print("  Running ANALYZE...")
        cursor.execute("ANALYZE")
        
        # Rebuild indexes
        print("  Rebuilding indexes...")
        cursor.execute("REINDEX")
        
        conn.close()
        
        # Check new size
        new_size = db_path.stat().st_size / (1024 * 1024)
        print(f"  💾 New database size: {new_size:.2f} MB")
        
        print("✅ Database optimization complete")
        
    except Exception as e:
        print(f"❌ Error optimizing database: {e}")

def main():
    """Main entry point for database utilities"""
    
    if len(sys.argv) < 2:
        print("\n" + "=" * 60)
        print("POLYMARKET TERMINAL - DATABASE UTILITIES")
        print("=" * 60)
        print("\nUsage:")
        print("  python backend/database/db_utils.py <command> [options]")
        print("\nCommands:")
        print("  status          - Check database status")
        print("  delete-tx       - Delete transaction data")
        print("  delete-pos      - Delete positions data")
        print("  force-close     - Force close connections")
        print("  optimize        - Optimize database (VACUUM, ANALYZE, REINDEX)")
        print("\nOptions:")
        print("  --db PATH       - Specify database path (default: project_root/polymarket_terminal.db)")
        print("\nExamples:")
        print("  python backend/database/db_utils.py status")
        print("  python backend/database/db_utils.py delete-tx")
        print("  python backend/database/db_utils.py optimize")
        sys.exit(1)
    
    # Parse database path if provided
    db_path = None
    if '--db' in sys.argv:
        idx = sys.argv.index('--db')
        if idx + 1 < len(sys.argv):
            db_path = Path(sys.argv[idx + 1])
    
    command = sys.argv[1]
    
    if command == 'status':
        check_database_status(db_path)
        
    elif command == 'delete-tx':
        response = input("\n⚠️ WARNING: This will delete all transaction data. Continue? (yes/no): ")
        if response.lower() == 'yes':
            success = delete_transaction_data(db_path)
            if not success:
                sys.exit(1)
        else:
            print("Operation cancelled")
    
    elif command == 'delete-pos':
        response = input("\n⚠️ WARNING: This will delete all positions data. Continue? (yes/no): ")
        if response.lower() == 'yes':
            success = delete_positions_data(db_path)
            if not success:
                sys.exit(1)
        else:
            print("Operation cancelled")
            
    elif command == 'force-close':
        force_close_database_connections(db_path)
    
    elif command == 'optimize':
        optimize_database(db_path)
        
    else:
        print(f"Unknown command: {command}")
        print("Run without arguments to see usage")
        sys.exit(1)

if __name__ == "__main__":
    main()