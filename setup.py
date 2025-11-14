#!/usr/bin/env python3
"""
Setup script for Polymarket Terminal Backend
Enhanced with selective loading and deletion capabilities
"""

import sys
import argparse
from pathlib import Path
import time
import logging

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.data_fetcher import PolymarketDataFetcher
from backend.database_schema import create_complete_schema


def setup_environment():
    """Setup environment variables if .env file doesn't exist"""
    env_file = Path('.env')

    if not env_file.exists():
        print("Creating .env file with default configuration...")

        default_env = """# Polymarket Terminal Configuration

# Database
DATABASE_PATH=polymarket_terminal.db

# API URLs
GAMMA_API_URL=https://gamma-api.polymarket.com
DATA_API_URL=https://data-api.polymarket.com
CLOB_API_URL=https://clob.polymarket.com

# WebSocket (for future use)
WS_URL=wss://ws.polymarket.com

# Logging
LOG_LEVEL=INFO
LOG_FILE=polymarket_fetcher.log

# Daily Scan Settings
ENABLE_DAILY_SCAN=true
DAILY_SCAN_TIME=02:00

# Feature Flags
FETCH_CLOSED_EVENTS=false
FETCH_ARCHIVED=false
FETCH_LIVE_VOLUME=true
FETCH_OPEN_INTEREST=true
FETCH_SERIES=true
FETCH_TAGS=true

# Limits
MAX_EVENTS_PER_RUN=1000
MAX_MARKETS_PER_EVENT=100
"""

        with open(env_file, 'w') as f:
            f.write(default_env)

        print("✅ .env file created with default settings")
        print("   Please edit .env file to customize your configuration")
        print("")


def initialize_database():
    """Initialize database with complete schema"""
    print("Initializing database schema...")
    try:
        create_complete_schema()
        print("✅ Database schema initialized successfully")
        return True
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        return False


def print_selective_help():
    """Print help for selective operations"""
    print("\n" + "=" * 70)
    print("SELECTIVE OPERATIONS GUIDE")
    print("=" * 70)
    print("\nYou can now load or delete specific data types individually:")
    print("\n📊 Available Data Types:")
    print("  • tags     - Market and event categorization tags")
    print("  • series   - Grouped collections of related events")
    print("  • events   - Prediction markets events")
    print("  • markets  - Individual prediction markets")
    print("  • users    - User profiles (primarily whales)")
    print("  • transactions - Trading transactions")
    print("  • comments - Event comments and reactions")
    print("\n💡 Example Commands:")
    print("  python setup.py --load-tags        # Load only tags")
    print("  python setup.py --delete-markets   # Delete only markets")
    print("  python setup.py --load-users       # Load only whale users")
    print("\n⚠️  Note: Some data types depend on others:")
    print("  • Markets require Events")
    print("  • Users require Markets")
    print("  • Transactions require Markets")
    print("  • Comments require Events")
    print("=" * 70)


def main():
    """Main setup and run script with selective operations"""

    parser = argparse.ArgumentParser(
        description='Polymarket Terminal Backend - Enhanced Setup and Management',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
BASIC OPERATIONS:
  # First time setup
  python setup.py --setup

  # Fetch all data (initial load)
  python setup.py --initial-load

  # Run daily update
  python setup.py --daily-update

  # Start scheduler for automatic daily updates
  python setup.py --scheduler

  # Reset and reload everything
  python setup.py --reset --initial-load

SELECTIVE LOADING:
  # Load specific data types
  python setup.py --load-tags
  python setup.py --load-series
  python setup.py --load-events
  python setup.py --load-markets
  python setup.py --load-users
  python setup.py --load-transactions
  python setup.py --load-comments

  # Load multiple data types
  python setup.py --load-events --load-markets --load-users

SELECTIVE DELETION:
  # Delete specific data types
  python setup.py --delete-tags
  python setup.py --delete-series
  python setup.py --delete-events
  python setup.py --delete-markets
  python setup.py --delete-users
  python setup.py --delete-transactions
  python setup.py --delete-comments

  # Delete multiple data types
  python setup.py --delete-users --delete-transactions

UTILITIES:
  # Show database statistics
  python setup.py --stats

  # Show help for selective operations
  python setup.py --help-selective
        """
    )

    # Basic operations
    parser.add_argument('--setup', action='store_true',
                        help='Initialize database schema and environment')
    parser.add_argument('--initial-load', action='store_true',
                        help='Perform initial data load from Polymarket (all data)')
    parser.add_argument('--daily-update', action='store_true',
                        help='Run daily update scan')
    parser.add_argument('--scheduler', action='store_true',
                        help='Start scheduler for automatic daily updates')
    parser.add_argument('--reset', action='store_true',
                        help='Reset database (clear all data)')
    parser.add_argument('--stats', action='store_true',
                        help='Show database statistics')
    
    # Selective loading operations
    load_group = parser.add_argument_group('selective loading')
    load_group.add_argument('--load-tags', action='store_true',
                           help='Load only tags data')
    load_group.add_argument('--load-series', action='store_true',
                           help='Load only series data')
    load_group.add_argument('--load-events', action='store_true',
                           help='Load only events data')
    load_group.add_argument('--load-markets', action='store_true',
                           help='Load only markets data')
    load_group.add_argument('--load-users', action='store_true',
                           help='Load only users data (whales)')
    load_group.add_argument('--load-transactions', action='store_true',
                           help='Load only transactions data')
    load_group.add_argument('--load-comments', action='store_true',
                           help='Load only comments data')
    
    # Selective deletion operations
    delete_group = parser.add_argument_group('selective deletion')
    delete_group.add_argument('--delete-tags', action='store_true',
                             help='Delete only tags data')
    delete_group.add_argument('--delete-series', action='store_true',
                             help='Delete only series data')
    delete_group.add_argument('--delete-events', action='store_true',
                             help='Delete only events data')
    delete_group.add_argument('--delete-markets', action='store_true',
                             help='Delete only markets data')
    delete_group.add_argument('--delete-users', action='store_true',
                             help='Delete only users data')
    delete_group.add_argument('--delete-transactions', action='store_true',
                             help='Delete only transactions data')
    delete_group.add_argument('--delete-comments', action='store_true',
                             help='Delete only comments data')
    
    # Options
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('--help-selective', action='store_true',
                        help='Show detailed help for selective operations')
    parser.add_argument('--closed-events', action='store_true',
                        help='Include closed events when loading events')
    parser.add_argument('--limit-markets', type=int, default=20,
                        help='Limit number of markets for transaction loading (default: 20)')

    args = parser.parse_args()

    # Show selective help if requested
    if args.help_selective:
        print_selective_help()
        return

    # Setup environment if needed
    setup_environment()

    # Configure logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # No action specified
    if not any(vars(args).values()):
        parser.print_help()
        return

    # Handle setup
    if args.setup:
        print("\n🚀 Setting up Polymarket Terminal Backend...")
        print("=" * 60)

        if not initialize_database():
            print("❌ Setup failed")
            sys.exit(1)

        print("\n✅ Setup complete! You can now:")
        print("  1. Run 'python setup.py --initial-load' to fetch all data")
        print("  2. Run 'python setup.py --load-events' to load only events")
        print("  3. Run 'python setup.py --help-selective' for selective operations guide")
        print("  4. Run 'python setup.py --scheduler' to start automatic updates")
        print("  5. Run 'python app.py' to start the Flask API server")
        return

    # Create fetcher instance for other operations
    fetcher = PolymarketDataFetcher()

    # Handle statistics
    if args.stats:
        stats = fetcher.get_statistics()
        print("\n📊 Database Statistics:")
        print("=" * 40)
        for table, count in stats.items():
            print(f"  {table:<25} {count:>10,} records")
        print("=" * 40)
        
        # Add whale-specific stats
        whale_count = fetcher.db_manager.fetch_one("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
        if whale_count:
            print(f"\n🐋 Whale Users: {whale_count['count']:,}")
        
        return

    # Handle reset
    if args.reset:
        response = input("\n⚠️  WARNING: This will delete all data. Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled")
            return
        fetcher.reset_database()

        # If not doing initial load after reset, exit
        if not args.initial_load:
            return

    # Handle initial load (all data at once)
    if args.initial_load:
        print("\n📄 Starting initial data load...")
        print("This may take 10-30 minutes depending on the amount of data")
        print("=" * 60)

        success = fetcher.initial_data_load(reset_database=False)
        if success:
            print("\n✅ Initial data load successful!")
        else:
            print("\n❌ Initial data load failed!")
            sys.exit(1)
        return

    # Handle selective loading operations
    selective_loads = []
    
    if args.load_tags:
        selective_loads.append(('tags', fetcher.load_tags_only))
    if args.load_series:
        selective_loads.append(('series', fetcher.load_series_only))
    if args.load_events:
        selective_loads.append(('events', lambda: fetcher.load_events_only(closed=args.closed_events)))
    if args.load_markets:
        selective_loads.append(('markets', fetcher.load_markets_only))
    if args.load_users:
        selective_loads.append(('users', fetcher.load_users_only))
    if args.load_transactions:
        selective_loads.append(('transactions', lambda: fetcher.load_transactions_only(limit_markets=args.limit_markets)))
    if args.load_comments:
        selective_loads.append(('comments', fetcher.load_comments_only))
    
    if selective_loads:
        print("\n🔄 Starting selective data loading...")
        print("=" * 60)
        
        results = {}
        for name, load_func in selective_loads:
            print(f"\nLoading {name}...")
            result = load_func()
            results[name] = result
            
            if not result['success']:
                print(f"⚠️  Warning: Failed to load {name}")
                if result.get('error'):
                    print(f"   Error: {result['error']}")
        
        # Print summary
        print("\n" + "=" * 60)
        print("SELECTIVE LOAD SUMMARY:")
        print("=" * 60)
        
        for name, result in results.items():
            if result['success']:
                if 'count' in result:
                    print(f"  ✅ {name}: {result['count']} loaded")
                elif 'whales_found' in result:
                    print(f"  ✅ {name}: {result['whales_found']} whales, {result['enriched']} enriched")
                elif 'comments' in result:
                    print(f"  ✅ {name}: {result['comments']} comments, {result['reactions']} reactions")
                elif 'transactions' in result:
                    print(f"  ✅ {name}: {result['transactions']} from {result['markets_processed']} markets")
            else:
                print(f"  ❌ {name}: Failed")
        
        print("=" * 60)

    # Handle selective deletion operations
    selective_deletes = []
    
    if args.delete_tags:
        selective_deletes.append(('tags', fetcher.delete_tags_only))
    if args.delete_series:
        selective_deletes.append(('series', fetcher.delete_series_only))
    if args.delete_events:
        selective_deletes.append(('events', fetcher.delete_events_only))
    if args.delete_markets:
        selective_deletes.append(('markets', fetcher.delete_markets_only))
    if args.delete_users:
        selective_deletes.append(('users', fetcher.delete_users_only))
    if args.delete_transactions:
        selective_deletes.append(('transactions', fetcher.delete_transactions_only))
    if args.delete_comments:
        selective_deletes.append(('comments', fetcher.delete_comments_only))
    
    if selective_deletes:
        response = input(f"\n⚠️  WARNING: This will delete {len(selective_deletes)} data type(s). Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled")
            return
        
        print("\n🗑️ Starting selective data deletion...")
        print("=" * 60)
        
        results = {}
        for name, delete_func in selective_deletes:
            print(f"\nDeleting {name}...")
            result = delete_func()
            results[name] = result
            
            if not result['success']:
                print(f"⚠️  Warning: Failed to delete {name}")
                if result.get('error'):
                    print(f"   Error: {result['error']}")
        
        # Print summary
        print("\n" + "=" * 60)
        print("SELECTIVE DELETE SUMMARY:")
        print("=" * 60)
        
        for name, result in results.items():
            if result['success']:
                print(f"  ✅ {name}: {result['deleted']} deleted")
            else:
                print(f"  ❌ {name}: Failed")
        
        print("=" * 60)

    # Handle daily update
    if args.daily_update:
        print("\n📄 Running daily update...")
        results = fetcher.daily_update()
        if results:
            print("\n✅ Daily update successful!")
        else:
            print("\n❌ Daily update failed!")
            sys.exit(1)
        return

    # Handle scheduler
    if args.scheduler:
        print("\n⏰ Starting scheduler for automatic daily updates...")
        print("Press Ctrl+C to stop")
        print("=" * 60)

        fetcher.start_scheduler()

        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            print("\n\nScheduler stopped")
        return


if __name__ == "__main__":
    main()