#!/usr/bin/env python3
"""
Database utilities for Polymarket Terminal
Force close connections and perform clean operations
"""

import sqlite3
import sys
import os
import time
import psutil  # You may need to install this: pip install psutil

def force_close_database_connections(db_path='polymarket_terminal.db'):
    """Force close all connections to the database"""
    print(f"🔒 Force closing all connections to {db_path}...")
    
    # Method 1: Try to find and kill processes using the database
    try:
        import psutil
        current_pid = os.getpid()
        
        for proc in psutil.process_iter(['pid', 'name', 'open_files']):
            try:
                # Check if this process has the database file open
                if proc.info['open_files']:
                    for file in proc.info['open_files']:
                        if db_path in file.path and proc.info['pid'] != current_pid:
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
    wal_file = f"{db_path}-wal"
    shm_file = f"{db_path}-shm"
    
    if os.path.exists(wal_file):
        print(f"  Removing WAL file: {wal_file}")
        try:
            os.remove(wal_file)
        except:
            print("    Could not remove WAL file")
    
    if os.path.exists(shm_file):
        print(f"  Removing SHM file: {shm_file}")
        try:
            os.remove(shm_file)
        except:
            print("    Could not remove SHM file")
    
    # Small delay to ensure cleanup
    time.sleep(1)
    print("✅ Connection cleanup complete")

def delete_transaction_data(db_path='polymarket_terminal.db'):
    """Delete all transaction and trading data with exclusive access"""
    
    # First force close any existing connections
    force_close_database_connections(db_path)
    
    print("\n🗑️ Deleting transaction and trading data...")
    
    try:
        # Connect with exclusive lock
        conn = sqlite3.connect(
            db_path,
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
                    print(f"  ⏭️  {table} is already empty")
                    
            except sqlite3.OperationalError as e:
                if "no such table" in str(e):
                    print(f"  ⚠️  Table {table} doesn't exist")
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

def check_database_status(db_path='polymarket_terminal.db'):
    """Check the status of the database and tables"""
    print(f"\n📊 Checking database status: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path, timeout=5.0)
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
        
        # Check transaction tables
        tables = [
            'transactions',
            'user_activity', 
            'user_trades',
            'user_positions_current',
            'user_positions_closed',
            'user_values'
        ]
        
        print("\n  Transaction table counts:")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"    {table:<25} {count:>10,} records")
            except:
                print(f"    {table:<25} (not found)")
        
        conn.close()
        
    except sqlite3.OperationalError as e:
        if "locked" in str(e):
            print("  ❌ Database is LOCKED - another process is using it")
        else:
            print(f"  ❌ Error accessing database: {e}")
    except Exception as e:
        print(f"  ❌ Unexpected error: {e}")

def main():
    """Main entry point for database utilities"""
    
    if len(sys.argv) < 2:
        print("Polymarket Terminal - Database Utilities")
        print("=" * 50)
        print("\nUsage:")
        print("  python db_utils.py status     - Check database status")
        print("  python db_utils.py delete-tx  - Delete transaction data")
        print("  python db_utils.py force-close - Force close connections")
        print("\nOptions:")
        print("  --db PATH                      - Specify database path")
        sys.exit(1)
    
    # Parse database path if provided
    db_path = 'polymarket_terminal.db'
    if '--db' in sys.argv:
        idx = sys.argv.index('--db')
        if idx + 1 < len(sys.argv):
            db_path = sys.argv[idx + 1]
    
    command = sys.argv[1]
    
    if command == 'status':
        check_database_status(db_path)
        
    elif command == 'delete-tx':
        response = input("\n⚠️  WARNING: This will delete all transaction data. Continue? (yes/no): ")
        if response.lower() == 'yes':
            success = delete_transaction_data(db_path)
            if not success:
                sys.exit(1)
        else:
            print("Operation cancelled")
            
    elif command == 'force-close':
        force_close_database_connections(db_path)
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()