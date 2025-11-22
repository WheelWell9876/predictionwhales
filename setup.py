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
        # Suppress verbose logging during setup
        import logging
        logging.getLogger('DatabaseManager').setLevel(logging.WARNING)
        
        db = DatabaseManager()
        
        # Get statistics about what was created
        tables = db.fetch_all("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        
        indexes = db.fetch_all("""
            SELECT name FROM sqlite_master 
            WHERE type='index' 
            ORDER BY name
        """)
        
        print(f"\n✅ Database initialized successfully")
        print(f"   📊 Tables created: {len(tables)}")
        print(f"   🔍 Indexes created: {len(indexes)}")
        
        # Only show errors if any tables are missing
        expected_tables = [
            'events', 'markets', 'series', 'tags', 'categories', 'collections',
            'event_tags', 'market_tags', 'series_tags', 'users', 'comments'
        ]
        
        table_names = [t['name'] for t in tables]
        missing = [t for t in expected_tables if t not in table_names]
        
        if missing:
            print(f"\n⚠️  Missing tables: {', '.join(missing)}")
        
        return True
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def verify_tables_for_operation(operation: str, section: str) -> bool:
    """Verify required tables exist for an operation"""
    db = DatabaseManager()
    
    # Define required tables for each section
    required_tables = {
        'events': ['events', 'tags', 'event_tags'],
        'markets': ['markets', 'events', 'tags', 'market_tags', 'categories', 'series'],
        'series': ['series', 'series_events', 'series_tags', 'series_categories'],
        'tags': ['tags', 'event_tags', 'market_tags', 'series_tags'],
        'users': ['users'],
        'comments': ['comments', 'events'],
        'positions': ['user_positions_current', 'user_positions_closed', 'users'],
        'transactions': ['transactions', 'users', 'markets']
    }
    
    tables_needed = required_tables.get(section, [])
    if not tables_needed:
        return True
    
    # Check if tables exist
    existing_tables = db.fetch_all("""
        SELECT name FROM sqlite_master 
        WHERE type='table'
    """)
    existing_names = [t['name'] for t in existing_tables]
    
    missing = [t for t in tables_needed if t not in existing_names]
    
    if missing:
        print(f"⚠️  Missing required tables for {section}: {', '.join(missing)}")
        return False
    
    return True

def load_section(fetcher: PolymarketDataFetcher, section: str, **kwargs):
    """Load a specific data section"""
    # Verify tables first
    if not verify_tables_for_operation('load', section):
        print(f"❌ Cannot load {section}: required tables missing")
        return False
    
    print(f"\n📥 Loading {section.upper()}")
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
            print(f"✅ {section.upper()} loaded in {elapsed:.2f} seconds")
            return True
        else:
            print(f"❌ Failed to load {section}: {result.get('error', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"❌ Error loading {section}: {e}")
        return False

def delete_section(fetcher: PolymarketDataFetcher, section: str, **kwargs):
    """Delete a specific data section"""
    # Verify tables first
    if not verify_tables_for_operation('delete', section):
        print(f"❌ Cannot delete {section}: required tables missing")
        return False
    
    print(f"\n🗑️ Deleting {section.upper()}")
    
    # Confirm deletion
    response = input(f"⚠️  Delete all {section} data? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        return False
    
    try:
        if section == "events":
            # Only delete from events table, not related tables
            result = fetcher.delete_events_only(keep_active=kwargs.get('keep_active', False))
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
            print(f"✅ {section.upper()} deleted")
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
    print("🚀 INITIAL DATA LOAD")
    print("=" * 80)
    
    response = input("\n⚠️  This will take considerable time. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        return False
    
    # Suppress verbose logging
    import logging
    logging.getLogger('DatabaseManager').setLevel(logging.WARNING)
    logging.getLogger('EventsManager').setLevel(logging.WARNING)
    logging.getLogger('MarketsManager').setLevel(logging.WARNING)
    
    fetcher = PolymarketDataFetcher()
    start_time = time.time()
    
    steps = [
        ("events", lambda: load_section(fetcher, "events", closed=False)),
        ("markets", lambda: load_section(fetcher, "markets")),
        ("series", lambda: load_section(fetcher, "series")),
        ("tags", lambda: load_section(fetcher, "tags")),
    ]
    
    completed = 0
    failed = []
    
    for step_name, step_func in steps:
        print(f"\n[{completed + 1}/{len(steps)}] {step_name.upper()}")
        
        if step_func():
            completed += 1
        else:
            failed.append(step_name)
            completed += 1
    
    elapsed = time.time() - start_time
    
    print("\n" + "=" * 80)
    print("📊 SUMMARY")
    print(f"✅ Completed: {completed}/{len(steps)} steps")
    if failed:
        print(f"⚠️  Failed: {', '.join(failed)}")
    print(f"⏱️  Time: {elapsed/60:.2f} minutes")
    
    return len(failed) == 0

def show_database_status():
    """Show current database status with table checkmarks"""
    db = DatabaseManager()
    
    print("\n" + "=" * 60)
    print("📊 DATABASE STATUS")
    print("=" * 60)
    
    # Define all expected tables
    all_tables = [
        'events', 'markets', 'series', 'tags', 'categories', 'collections',
        'event_tags', 'market_tags', 'series_tags', 'event_categories',
        'market_categories', 'series_categories', 'event_series', 'event_collections',
        'series_collections', 'users', 'comments', 'transactions',
        'user_positions_current', 'user_positions_closed', 'user_activity',
        'user_trades', 'user_values', 'event_live_volume', 'market_open_interest',
        'market_holders', 'image_optimized', 'event_creators', 'chats',
        'templates', 'comment_reactions', 'tag_relationships'
    ]
    
    # Get existing tables
    existing = db.fetch_all("SELECT name FROM sqlite_master WHERE type='table'")
    existing_names = [t['name'] for t in existing]
    
    # Display in two columns
    print("\nTables:")
    for i in range(0, len(all_tables), 2):
        table1 = all_tables[i]
        status1 = "✅" if table1 in existing_names else "❌"
        
        if i + 1 < len(all_tables):
            table2 = all_tables[i + 1]
            status2 = "✅" if table2 in existing_names else "❌"
            print(f"  {status1} {table1:<30} {status2} {table2}")
        else:
            print(f"  {status1} {table1}")
    
    # Show record counts for main tables
    print("\nRecord Counts:")
    main_tables = ['events', 'markets', 'series', 'tags', 'users']
    for table in main_tables:
        if table in existing_names:
            count = db.fetch_one(f"SELECT COUNT(*) as c FROM {table}")
            print(f"  {table:<15} {count['c']:>10,} records")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Polymarket Terminal Setup')
    
    # Commands
    parser.add_argument('--setup', action='store_true', help='Initialize database')
    parser.add_argument('--status', action='store_true', help='Show database status')
    parser.add_argument('--initial-load', action='store_true', help='Complete initial load')
    
    # Load commands
    parser.add_argument('--load-events', action='store_true')
    parser.add_argument('--load-markets', action='store_true')
    parser.add_argument('--load-series', action='store_true')
    parser.add_argument('--load-tags', action='store_true')
    parser.add_argument('--load-users', action='store_true')
    parser.add_argument('--load-comments', action='store_true')
    parser.add_argument('--load-positions', action='store_true')
    parser.add_argument('--load-transactions', action='store_true')
    
    # Delete commands  
    parser.add_argument('--delete-events', action='store_true')
    parser.add_argument('--delete-markets', action='store_true')
    parser.add_argument('--delete-series', action='store_true')
    parser.add_argument('--delete-tags', action='store_true')
    parser.add_argument('--delete-users', action='store_true')
    parser.add_argument('--delete-comments', action='store_true')
    parser.add_argument('--delete-positions', action='store_true')
    parser.add_argument('--delete-transactions', action='store_true')
    
    # Options
    parser.add_argument('--closed', action='store_true')
    parser.add_argument('--all-users', action='store_true')
    parser.add_argument('--quick', action='store_true')
    
    args = parser.parse_args()
    
    # Suppress verbose logging by default
    import logging
    logging.getLogger('DatabaseManager').setLevel(logging.WARNING)
    logging.getLogger('EventsManager').setLevel(logging.WARNING)
    logging.getLogger('MarketsManager').setLevel(logging.WARNING)
    logging.getLogger('SeriesManager').setLevel(logging.WARNING)
    logging.getLogger('TagsManager').setLevel(logging.WARNING)
    
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    print("\n" + "=" * 80)
    print("🚀 POLYMARKET TERMINAL")
    print("=" * 80)
    
    # Handle commands
    if args.setup:
        setup_database()
        show_database_status()
    
    elif args.status:
        show_database_status()
    
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
            delete_section(fetcher, "events", keep_active=not args.closed)
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