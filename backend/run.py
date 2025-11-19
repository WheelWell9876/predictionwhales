#!/usr/bin/env python3
"""
Polymarket Terminal - Backend Runner
Main entry point for running the Polymarket data fetcher service
"""

import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# Add parent directory to path to import backend modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.database.data_fetcher import PolymarketDataFetcher
from backend.config import Config

def run_continuous():
    """Run the data fetcher in continuous mode with scheduled updates"""
    print("\n" + "=" * 80)
    print("🚀 POLYMARKET TERMINAL - CONTINUOUS MODE")
    print("=" * 80)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Update interval: Every {Config.FETCH_INTERVAL_MINUTES} minutes")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    fetcher = PolymarketDataFetcher()
    
    try:
        while True:
            print(f"\n⏰ Running update at {datetime.now().strftime('%H:%M:%S')}")
            
            # Run daily scan for all sections
            fetcher.run_daily_scan()
            
            # Wait for next interval
            print(f"\n💤 Sleeping for {Config.FETCH_INTERVAL_MINUTES} minutes...")
            time.sleep(Config.FETCH_INTERVAL_MINUTES * 60)
            
    except KeyboardInterrupt:
        print("\n\n⛔ Shutting down...")
        print("✅ Service stopped gracefully")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

def run_once(section: str = None):
    """Run the data fetcher once for specific section or all sections"""
    print("\n" + "=" * 80)
    print("🚀 POLYMARKET TERMINAL - SINGLE RUN")
    print("=" * 80)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    fetcher = PolymarketDataFetcher()
    
    if section:
        print(f"📌 Loading {section.upper()} only...")
        
        start_time = time.time()
        
        try:
            if section == "events":
                result = fetcher.load_events_only()
            elif section == "markets":
                result = fetcher.load_markets_only()
            elif section == "series":
                result = fetcher.load_series_only()
            elif section == "tags":
                result = fetcher.load_tags_only()
            elif section == "users":
                result = fetcher.load_users_only()
            elif section == "comments":
                result = fetcher.load_comments_only()
            elif section == "positions":
                result = fetcher.load_positions_only()
            elif section == "transactions":
                result = fetcher.load_transactions_only()
            else:
                print(f"❌ Unknown section: {section}")
                print("Valid sections: events, markets, series, tags, users, comments, positions, transactions")
                return
            
            elapsed = time.time() - start_time
            
            if result.get('success', False):
                print(f"✅ {section.upper()} loaded successfully in {elapsed:.2f} seconds")
            else:
                print(f"❌ Failed to load {section}: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Error loading {section}: {e}")
    else:
        print("📌 Running complete daily scan...")
        fetcher.run_daily_scan()
    
    print("\n✨ Done!")

def show_stats():
    """Display current database statistics"""
    print("\n" + "=" * 80)
    print("📊 DATABASE STATISTICS")
    print("=" * 80)
    
    from backend.database.database_manager import DatabaseManager
    db = DatabaseManager()
    
    sections = [
        ('events', 'Events'),
        ('markets', 'Markets'),
        ('series', 'Series'),
        ('tags', 'Tags'),
        ('users', 'Users'),
        ('comments', 'Comments'),
        ('user_positions_current', 'Current Positions'),
        ('user_positions_closed', 'Closed Positions'),
        ('transactions', 'Transactions'),
        ('user_activity', 'User Activity'),
        ('user_trades', 'User Trades')
    ]
    
    print("\n📈 Record Counts:")
    total_records = 0
    
    for table, name in sections:
        try:
            count = db.get_table_count(table)
            total_records += count
            print(f"   {name:<20} {count:>10,}")
        except:
            print(f"   {name:<20} {'N/A':>10}")
    
    print(f"\n   {'TOTAL':<20} {total_records:>10,}")
    
    # Database file size
    db_path = Path("polymarket_terminal.db")
    if db_path.exists():
        size_mb = db_path.stat().st_size / (1024 * 1024)
        print(f"\n💾 Database Size: {size_mb:.2f} MB")
    
    # Check for whale users
    try:
        whale_count = db.fetch_one("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
        if whale_count:
            print(f"\n🐋 Whale Users: {whale_count['count']:,}")
    except:
        pass
    
    # Check for active events
    try:
        active_events = db.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 0")
        closed_events = db.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
        if active_events:
            print(f"\n📊 Event Status:")
            print(f"   Active: {active_events['count']:,}")
            print(f"   Closed: {closed_events['count']:,}")
    except:
        pass
    
    print("\n" + "=" * 80)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Polymarket Terminal Backend Runner')
    
    # Mode selection
    parser.add_argument('--continuous', action='store_true', 
                       help='Run continuously with scheduled updates')
    parser.add_argument('--once', action='store_true', 
                       help='Run once and exit')
    parser.add_argument('--stats', action='store_true', 
                       help='Show database statistics')
    
    # Section selection for single run
    parser.add_argument('--section', type=str, 
                       choices=['events', 'markets', 'series', 'tags', 'users', 
                               'comments', 'positions', 'transactions'],
                       help='Load specific section only')
    
    # Options
    parser.add_argument('--debug', action='store_true', 
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set debug mode if requested
    if args.debug:
        import logging
        logging.basicConfig(level=logging.DEBUG)
    
    # Handle commands
    if args.stats:
        show_stats()
    elif args.continuous:
        run_continuous()
    elif args.once or args.section:
        run_once(args.section)
    else:
        # Default to showing help
        parser.print_help()
        print("\nExamples:")
        print("  python backend/run.py --once                  # Run complete scan once")
        print("  python backend/run.py --continuous            # Run continuously")
        print("  python backend/run.py --section events        # Load events only")
        print("  python backend/run.py --stats                 # Show statistics")

if __name__ == "__main__":
    main()