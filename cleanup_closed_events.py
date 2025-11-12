#!/usr/bin/env python3
"""
Cleanup Script - Remove Closed Events from Database
Run this to clean up the 48,862 closed events that were accidentally fetched
"""

import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent))

from backend.events_manager import EventsManager


def main():
    print("=" * 60)
    print("🧹 CLOSED EVENTS CLEANUP")
    print("=" * 60)
    
    events_mgr = EventsManager()
    
    # Show current stats
    print("\n📊 Current Database Stats:")
    
    total = events_mgr.fetch_one("SELECT COUNT(*) as count FROM events")
    closed = events_mgr.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 1")
    active = events_mgr.fetch_one("SELECT COUNT(*) as count FROM events WHERE closed = 0")
    
    print(f"   Total Events: {total['count']:,}")
    print(f"   Closed Events: {closed['count']:,}")
    print(f"   Active Events: {active['count']:,}")
    
    if closed['count'] == 0:
        print("\n✅ No closed events to remove!")
        return
    
    # Confirm deletion
    print(f"\n⚠️  This will DELETE {closed['count']:,} closed events")
    response = input("Continue? (yes/no): ")
    
    if response.lower() != 'yes':
        print("❌ Cancelled")
        return
    
    # Remove closed events
    print("\n🗑️  Removing closed events...")
    deleted = events_mgr.remove_closed_events()
    
    # Show new stats
    print("\n✅ Cleanup Complete!")
    
    remaining = events_mgr.fetch_one("SELECT COUNT(*) as count FROM events")
    print(f"   Deleted: {deleted:,} closed events")
    print(f"   Remaining: {remaining['count']:,} active events")
    
    # Vacuum database to reclaim space
    print("\n🔧 Optimizing database...")
    conn = events_mgr.get_persistent_connection()
    conn.execute("VACUUM")
    print("✅ Database optimized")
    
    print("\n" + "=" * 60)
    print("🎉 Done! Your database now only has active events")
    print("=" * 60)


if __name__ == "__main__":
    main()