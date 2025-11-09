#!/usr/bin/env python3
"""
Setup script for Polymarket Terminal Backend
Run this script to initialize the database and start fetching data
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


def main():
    """Main setup and run script"""

    parser = argparse.ArgumentParser(
        description='Polymarket Terminal Backend Setup and Management',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
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

  # Show database statistics
  python setup.py --stats
        """
    )

    parser.add_argument('--setup', action='store_true',
                        help='Initialize database schema and environment')
    parser.add_argument('--initial-load', action='store_true',
                        help='Perform initial data load from Polymarket')
    parser.add_argument('--daily-update', action='store_true',
                        help='Run daily update scan')
    parser.add_argument('--scheduler', action='store_true',
                        help='Start scheduler for automatic daily updates')
    parser.add_argument('--reset', action='store_true',
                        help='Reset database (clear all data)')
    parser.add_argument('--stats', action='store_true',
                        help='Show database statistics')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

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
        print("  2. Run 'python setup.py --scheduler' to start automatic updates")
        print("  3. Run 'python app.py' to start the Flask API server")
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

    # Handle initial load
    if args.initial_load:
        print("\n🔄 Starting initial data load...")
        print("This may take 10-30 minutes depending on the amount of data")
        print("=" * 60)

        success = fetcher.initial_data_load(reset_database=False)
        if success:
            print("\n✅ Initial data load successful!")
        else:
            print("\n❌ Initial data load failed!")
            sys.exit(1)
        return

    # Handle daily update
    if args.daily_update:
        print("\n🔄 Running daily update...")
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