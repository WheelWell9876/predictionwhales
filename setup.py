#!/usr/bin/env python3
"""
Polymarket Terminal Setup and Data Management
Main entry point for database setup and data loading operations
"""

import sys
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.database.database_manager import DatabaseManager
from backend.database.data_fetcher import PolymarketDataFetcher

def run_command(command: str, description: str):
    """Run a shell command and display the result"""
    print(f"\n📌 {description}...")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
        else:
            print(f"❌ Error in {description}: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Failed to run {description}: {e}")
        return False
    return True

def setup_database():
    """Initialize the database schema"""
    print("\n" + "=" * 60)
    print("🔧 SETTING UP DATABASE")
    print("=" * 60)
    
    try:
        db = DatabaseManager()
        # Initialize schema is already called in __init__
        print("✅ Database initialized successfully")
        
        # Show initial stats
        print("\n📊 Database Tables Created:")
        tables = db.fetch_all("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        for table in tables:
            print(f"   • {table['name']}")
        
        return True
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def load_section(fetcher: PolymarketDataFetcher, section: str, **kwargs):
    """Load a specific data section"""
    print("\n" + "=" * 60)
    print(f"📥 LOADING {section.upper()}")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        if section == "events":
            result = fetcher.load_events_only(closed=kwargs.get('closed', False))
        elif section == "markets":
            result = fetcher.load_markets_only(event_ids=kwargs.get('event_ids'))
        elif section == "series":
            result = fetcher.load_series_only()
        elif section == "tags":
            result = fetcher.load_tags_only()
        elif section == "users":
            result = fetcher.load_users_only(whales_only=kwargs.get('whales_only', True))
        elif section == "comments":
            result = fetcher.load_comments_only(limit_per_event=kwargs.get('limit_per_event', 15))
        elif section == "positions":
            result = fetcher.load_positions_only(whale_users_only=kwargs.get('whale_users_only', True))
        elif section == "transactions":
            result = fetcher.load_transactions_only(comprehensive=kwargs.get('comprehensive', True))
        else:
            print(f"❌ Unknown section: {section}")
            return False
        
        elapsed = time.time() - start_time
        
        if result.get('success', False):
            print(f"✅ {section.upper()} loaded successfully in {elapsed:.2f} seconds")
            return True
        else:
            print(f"❌ Failed to load {section}: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error loading {section}: {e}")
        return False

def delete_section(fetcher: PolymarketDataFetcher, section: str, **kwargs):
    """Delete a specific data section"""
    print("\n" + "=" * 60)
    print(f"🗑️ DELETING {section.upper()}")
    print("=" * 60)
    
    # Confirm deletion
    response = input(f"⚠️  Are you sure you want to delete all {section} data? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ Deletion cancelled")
        return False
    
    try:
        if section == "events":
            result = fetcher.delete_events_only(keep_active=kwargs.get('keep_active', True))
        elif section == "markets":
            result = fetcher.delete_markets_only()
        elif section == "series":
            result = fetcher.delete_series_only()
        elif section == "tags":
            result = fetcher.delete_tags_only()
        elif section == "users":
            result = fetcher.delete_users_only()
        elif section == "comments":
            result = fetcher.delete_comments_only()
        elif section == "positions":
            result = fetcher.delete_positions_only()
        elif section == "transactions":
            result = fetcher.delete_transactions_only()
        else:
            print(f"❌ Unknown section: {section}")
            return False
        
        if result.get('success', False):
            print(f"✅ {section.upper()} deleted successfully")
            return True
        else:
            print(f"❌ Failed to delete {section}: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error deleting {section}: {e}")
        return False

def initial_load():
    """Perform complete initial data load in correct sequence"""
    print("\n" + "=" * 80)
    print("🚀 INITIAL DATA LOAD - COMPLETE SEQUENTIAL LOAD")
    print("=" * 80)
    print("\nThis will load all data in the following order:")
    print("1. Events (active only)")
    print("2. Markets")
    print("3. Clean closed events")
    print("4. Series")
    print("5. Tags")
    print("6. Clean closed events again")
    print("7. Users (whales)")
    print("8. Comments")
    print("9. Positions (whales)")
    print("10. Transactions (comprehensive)")
    print("11. Analyze data")
    
    response = input("\n⚠️  This will take considerable time. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("❌ Initial load cancelled")
        return False
    
    fetcher = PolymarketDataFetcher()
    start_time = time.time()
    
    steps = [
        ("events", lambda: load_section(fetcher, "events", closed=False)),
        ("markets", lambda: load_section(fetcher, "markets")),
        ("cleanup1", lambda: run_command("python cleanup_closed_events.py", "Cleaning closed events (pass 1)")),
        ("series", lambda: load_section(fetcher, "series")),
        ("tags", lambda: load_section(fetcher, "tags")),
        ("cleanup2", lambda: run_command("python cleanup_closed_events.py", "Cleaning closed events (pass 2)")),
        ("users", lambda: load_section(fetcher, "users", whales_only=True)),
        ("comments", lambda: load_section(fetcher, "comments", limit_per_event=15)),
        ("positions", lambda: load_section(fetcher, "positions", whale_users_only=True)),
        ("transactions", lambda: load_section(fetcher, "transactions", comprehensive=True)),
        ("analyze", lambda: run_command(
            "python analyze_data.py --db polymarket_terminal.db --output report.json",
            "Analyzing data and generating report"
        ))
    ]
    
    completed = 0
    failed = []
    
    for step_name, step_func in steps:
        print(f"\n{'=' * 60}")
        print(f"Step {completed + 1}/{len(steps)}: {step_name.upper()}")
        print('=' * 60)
        
        if step_func():
            completed += 1
            print(f"✅ {step_name} completed ({completed}/{len(steps)})")
        else:
            failed.append(step_name)
            print(f"⚠️ {step_name} failed but continuing...")
            completed += 1
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 80)
    print("📊 INITIAL LOAD SUMMARY")
    print("=" * 80)
    print(f"✅ Completed: {completed}/{len(steps)} steps")
    if failed:
        print(f"⚠️  Failed steps: {', '.join(failed)}")
    print(f"⏱️  Total time: {elapsed/60:.2f} minutes")
    
    if not failed:
        print("\n🎉 Initial load completed successfully!")
    else:
        print(f"\n⚠️ Initial load completed with {len(failed)} failures")
    
    return len(failed) == 0

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Polymarket Terminal Setup and Data Management')
    
    # Setup command
    parser.add_argument('--setup', action='store_true', help='Initialize database schema')
    
    # Load commands
    parser.add_argument('--initial-load', action='store_true', help='Perform complete initial data load')
    parser.add_argument('--load-events', action='store_true', help='Load events data')
    parser.add_argument('--load-markets', action='store_true', help='Load markets data')
    parser.add_argument('--load-series', action='store_true', help='Load series data')
    parser.add_argument('--load-tags', action='store_true', help='Load tags data')
    parser.add_argument('--load-users', action='store_true', help='Load users data')
    parser.add_argument('--load-comments', action='store_true', help='Load comments data')
    parser.add_argument('--load-positions', action='store_true', help='Load positions data')
    parser.add_argument('--load-transactions', action='store_true', help='Load transactions data')
    
    # Delete commands
    parser.add_argument('--delete-events', action='store_true', help='Delete events data')
    parser.add_argument('--delete-markets', action='store_true', help='Delete markets data')
    parser.add_argument('--delete-series', action='store_true', help='Delete series data')
    parser.add_argument('--delete-tags', action='store_true', help='Delete tags data')
    parser.add_argument('--delete-users', action='store_true', help='Delete users data')
    parser.add_argument('--delete-comments', action='store_true', help='Delete comments data')
    parser.add_argument('--delete-positions', action='store_true', help='Delete positions data')
    parser.add_argument('--delete-transactions', action='store_true', help='Delete transactions data')
    
    # Options
    parser.add_argument('--closed', action='store_true', help='Include closed events when loading')
    parser.add_argument('--all-users', action='store_true', help='Load all users, not just whales')
    parser.add_argument('--quick', action='store_true', help='Quick mode for transactions (not comprehensive)')
    
    args = parser.parse_args()
    
    # Check if any argument was provided
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    print("\n" + "=" * 80)
    print("🚀 POLYMARKET TERMINAL - DATA MANAGEMENT")
    print("=" * 80)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Handle commands
    if args.setup:
        setup_database()
    
    elif args.initial_load:
        initial_load()
    
    else:
        # Individual operations
        fetcher = PolymarketDataFetcher()
        
        # Load operations
        if args.load_events:
            load_section(fetcher, "events", closed=args.closed)
        if args.load_markets:
            load_section(fetcher, "markets")
        if args.load_series:
            load_section(fetcher, "series")
        if args.load_tags:
            load_section(fetcher, "tags")
        if args.load_users:
            load_section(fetcher, "users", whales_only=not args.all_users)
        if args.load_comments:
            load_section(fetcher, "comments")
        if args.load_positions:
            load_section(fetcher, "positions", whale_users_only=not args.all_users)
        if args.load_transactions:
            load_section(fetcher, "transactions", comprehensive=not args.quick)
        
        # Delete operations
        if args.delete_events:
            delete_section(fetcher, "events")
        if args.delete_markets:
            delete_section(fetcher, "markets")
        if args.delete_series:
            delete_section(fetcher, "series")
        if args.delete_tags:
            delete_section(fetcher, "tags")
        if args.delete_users:
            delete_section(fetcher, "users")
        if args.delete_comments:
            delete_section(fetcher, "comments")
        if args.delete_positions:
            delete_section(fetcher, "positions")
        if args.delete_transactions:
            delete_section(fetcher, "transactions")
    
    print("\n✨ Done!")

if __name__ == "__main__":
    main()