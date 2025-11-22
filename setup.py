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
        
        return True
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

def load_section(fetcher: PolymarketDataFetcher, section: str, **kwargs):
    """Load a specific data section"""
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
        elif section == "tags-with-relationships":
            result = fetcher.load_tags_with_relationships()
        elif section == "tag-relationships":
            result = fetcher.load_tag_relationships_only()
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
    print(f"\n🗑️ Deleting {section.upper()}")
    
    # Confirm deletion
    response = input(f"⚠️  Delete all {section} data? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled")
        return False
    
    try:
        if section == "events":
            result = fetcher.delete_events_only(keep_active=kwargs.get('keep_active', False))
        elif section == "markets":
            result = fetcher.delete_markets_only()
        elif section == "series":
            result = fetcher.delete_series_only()
        elif section == "tags":
            result = fetcher.delete_tags_only()
        elif section == "tag-relationships":
            result = fetcher.delete_tag_relationships_only()
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

def show_database_status():
    """Show current database status with table checkmarks"""
    db = DatabaseManager()
    
    print("\n" + "=" * 60)
    print("📊 DATABASE STATUS")
    print("=" * 60)
    
    # Define all expected tables
    all_tables = [
        'events', 'markets', 'series', 'tags', 'categories', 'collections',
        'event_tags', 'market_tags', 'series_tags', 'series_events',
        'series_categories', 'series_collections', 'series_chats',
        'tag_relationships', 'users', 'comments', 'chats',
        'event_live_volume'
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
    main_tables = ['events', 'markets', 'series', 'tags', 'tag_relationships', 'users']
    for table in main_tables:
        if table in existing_names:
            count = db.fetch_one(f"SELECT COUNT(*) as c FROM {table}")
            print(f"  {table:<20} {count['c']:>10,} records")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Polymarket Terminal Setup')
    
    # Commands
    parser.add_argument('--setup', action='store_true', help='Initialize database')
    parser.add_argument('--status', action='store_true', help='Show database status')
    
    # Load commands
    parser.add_argument('--load-core', action='store_true', help='Load events, series, tags, and markets in correct order')
    parser.add_argument('--load-events', action='store_true')
    parser.add_argument('--load-markets', action='store_true')
    parser.add_argument('--load-series', action='store_true')
    parser.add_argument('--load-tags', action='store_true')
    parser.add_argument('--load-tags-with-relationships', action='store_true', help='Load tags and then tag relationships')
    parser.add_argument('--load-tag-relationships', action='store_true', help='Load tag relationships from events')
    parser.add_argument('--load-users', action='store_true')
    parser.add_argument('--load-comments', action='store_true')
    parser.add_argument('--load-positions', action='store_true')
    parser.add_argument('--load-transactions', action='store_true')
    
    # Delete commands
    parser.add_argument('--delete-events', action='store_true')
    parser.add_argument('--delete-markets', action='store_true')
    parser.add_argument('--delete-series', action='store_true')
    parser.add_argument('--delete-tags', action='store_true')
    parser.add_argument('--delete-tag-relationships', action='store_true')
    parser.add_argument('--delete-users', action='store_true')
    parser.add_argument('--delete-comments', action='store_true')
    parser.add_argument('--delete-positions', action='store_true')
    parser.add_argument('--delete-transactions', action='store_true')

    # Reset commands (drop and recreate tables)
    parser.add_argument('--reset-db', action='store_true', help='Reset entire database (drop all tables and recreate)')
    parser.add_argument('--reset-table', type=str, metavar='TABLE', help='Reset a specific table (drop and recreate)')
    parser.add_argument('--reset-tags-tables', action='store_true', help='Reset all tag-related tables')
    parser.add_argument('--reset-core-tables', action='store_true', help='Reset core tables (events, markets, series, tags)')
    parser.add_argument('--list-tables', action='store_true', help='List all available tables')

    # Options
    parser.add_argument('--closed', action='store_true')
    parser.add_argument('--all-users', action='store_true')
    parser.add_argument('--quick', action='store_true')
    parser.add_argument('--force', action='store_true', help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    # Suppress verbose logging by default
    import logging
    logging.getLogger('DatabaseManager').setLevel(logging.WARNING)
    logging.getLogger('EventsManager').setLevel(logging.WARNING)
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
    
    else:
        # Individual operations
        fetcher = PolymarketDataFetcher()

        # Combined load operation
        if args.load_core:
            print("\n📥 Loading CORE DATA (events → series → tags → markets)")
            start_time = time.time()
            results = fetcher.load_core_data()
            elapsed = time.time() - start_time

            # Summary
            success_count = sum(1 for r in results.values() if r.get('success', False))
            print(f"\n✅ Core data loaded: {success_count}/{len(results)} sections in {elapsed:.2f} seconds")
            for section, result in results.items():
                status = "✅" if result.get('success', False) else "❌"
                count = result.get('count', 0)
                print(f"   {status} {section}: {count} records")

        # Load operations
        if args.load_events:
            load_section(fetcher, "events", closed=args.closed)
        if args.load_markets:
            load_section(fetcher, "markets")
        if args.load_series:
            load_section(fetcher, "series")
        if args.load_tags:
            load_section(fetcher, "tags")
        if args.load_tags_with_relationships:
            load_section(fetcher, "tags-with-relationships")
        if args.load_tag_relationships:
            load_section(fetcher, "tag-relationships")
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
        if args.delete_tag_relationships:
            delete_section(fetcher, "tag-relationships")
        if args.delete_users:
            delete_section(fetcher, "users")
        if args.delete_comments:
            delete_section(fetcher, "comments")
        if args.delete_positions:
            delete_section(fetcher, "positions")
        if args.delete_transactions:
            delete_section(fetcher, "transactions")

        # Reset operations (drop and recreate tables)
        db = DatabaseManager()

        if args.list_tables:
            print("\n📋 Available Tables:")
            tables = db.fetch_all("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            for i, t in enumerate(tables, 1):
                count = db.get_table_count(t['name'])
                print(f"  {i:2}. {t['name']:<30} ({count:,} rows)")

        if args.reset_db:
            print("\n🔄 RESETTING ENTIRE DATABASE")
            if not args.force:
                response = input("⚠️  This will DROP ALL TABLES and recreate them. Continue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled")
                else:
                    result = db.reset_database()
                    if result['success']:
                        print(f"✅ Database reset complete: {result['dropped']} tables dropped, {result['created']} tables created")
                    else:
                        print(f"❌ Reset failed: {result.get('error', 'Unknown error')}")
            else:
                result = db.reset_database()
                if result['success']:
                    print(f"✅ Database reset complete: {result['dropped']} tables dropped, {result['created']} tables created")
                else:
                    print(f"❌ Reset failed: {result.get('error', 'Unknown error')}")

        if args.reset_table:
            table_name = args.reset_table
            print(f"\n🔄 RESETTING TABLE: {table_name}")
            if not args.force:
                response = input(f"⚠️  This will DROP and RECREATE the '{table_name}' table. Continue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled")
                else:
                    result = db.reset_table(table_name)
                    if result['success']:
                        print(f"✅ Table '{table_name}' reset successfully")
                    else:
                        print(f"❌ Reset failed: {result.get('error', 'Unknown error')}")
            else:
                result = db.reset_table(table_name)
                if result['success']:
                    print(f"✅ Table '{table_name}' reset successfully")
                else:
                    print(f"❌ Reset failed: {result.get('error', 'Unknown error')}")

        if args.reset_tags_tables:
            tag_tables = ['tags', 'tag_relationships', 'event_tags', 'market_tags', 'series_tags']
            print(f"\n🔄 RESETTING TAG-RELATED TABLES: {', '.join(tag_tables)}")
            if not args.force:
                response = input(f"⚠️  This will DROP and RECREATE {len(tag_tables)} tables. Continue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled")
                else:
                    result = db.reset_tables(tag_tables)
                    if result['success']:
                        print(f"✅ Reset {len(result['reset'])} tag tables successfully")
                    else:
                        print(f"❌ Some errors: {result.get('errors', [])}")
            else:
                result = db.reset_tables(tag_tables)
                if result['success']:
                    print(f"✅ Reset {len(result['reset'])} tag tables successfully")
                else:
                    print(f"❌ Some errors: {result.get('errors', [])}")

        if args.reset_core_tables:
            core_tables = ['events', 'markets', 'series', 'tags', 'event_tags', 'market_tags', 'series_tags', 'series_events']
            print(f"\n🔄 RESETTING CORE TABLES: {', '.join(core_tables)}")
            if not args.force:
                response = input(f"⚠️  This will DROP and RECREATE {len(core_tables)} tables. Continue? (yes/no): ")
                if response.lower() != 'yes':
                    print("Cancelled")
                else:
                    result = db.reset_tables(core_tables)
                    if result['success']:
                        print(f"✅ Reset {len(result['reset'])} core tables successfully")
                    else:
                        print(f"❌ Some errors: {result.get('errors', [])}")
            else:
                result = db.reset_tables(core_tables)
                if result['success']:
                    print(f"✅ Reset {len(result['reset'])} core tables successfully")
                else:
                    print(f"❌ Some errors: {result.get('errors', [])}")

    print("\n✨ Done!")

if __name__ == "__main__":
    main()