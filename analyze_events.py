"""
Enhanced Database Analysis for Events Issue Detection
Specifically checks for duplicate events and closed events issues
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List


class EventsAnalyzer:
    """Analyze events table for duplicates and issues"""

    def __init__(self, db_path: str = 'polymarket_terminal.db'):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect to the database"""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        return self.conn

    def disconnect(self):
        """Disconnect from the database"""
        if self.conn:
            self.conn.close()

    def check_duplicate_events(self) -> Dict:
        """Check for duplicate events by ID"""
        cursor = self.conn.cursor()

        print("\n🔍 Checking for Duplicate Events...")
        print("=" * 60)

        # Check for duplicate IDs (should never happen with PRIMARY KEY)
        cursor.execute("""
            SELECT id, COUNT(*) as count
            FROM events
            GROUP BY id
            HAVING count > 1
            ORDER BY count DESC
        """)
        duplicates_by_id = cursor.fetchall()

        # Check for duplicate slugs
        cursor.execute("""
            SELECT slug, COUNT(*) as count, GROUP_CONCAT(id) as event_ids
            FROM events
            WHERE slug IS NOT NULL
            GROUP BY slug
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 20
        """)
        duplicates_by_slug = cursor.fetchall()

        # Check for duplicate titles
        cursor.execute("""
            SELECT title, COUNT(*) as count
            FROM events
            WHERE title IS NOT NULL
            GROUP BY title
            HAVING count > 1
            ORDER BY count DESC
            LIMIT 20
        """)
        duplicates_by_title = cursor.fetchall()

        print(f"Duplicate IDs (should be 0): {len(duplicates_by_id)}")
        if duplicates_by_id:
            print("⚠️  WARNING: Found duplicate event IDs!")
            for row in duplicates_by_id[:10]:
                print(f"  ID {row[0]}: {row[1]} occurrences")

        print(f"\nDuplicate Slugs: {len(duplicates_by_slug)}")
        if duplicates_by_slug:
            for row in duplicates_by_slug[:5]:
                print(f"  Slug '{row[0]}': {row[1]} occurrences (IDs: {row[2]})")

        print(f"\nDuplicate Titles: {len(duplicates_by_title)}")
        if duplicates_by_title:
            for row in duplicates_by_title[:5]:
                print(f"  Title '{row[0][:50]}...': {row[1]} occurrences")

        return {
            'duplicate_ids': len(duplicates_by_id),
            'duplicate_slugs': len(duplicates_by_slug),
            'duplicate_titles': len(duplicates_by_title)
        }

    def analyze_closed_events(self) -> Dict:
        """Analyze closed vs active events"""
        cursor = self.conn.cursor()

        print("\n📊 Analyzing Closed vs Active Events...")
        print("=" * 60)

        # Count by closed status
        cursor.execute("""
            SELECT closed, COUNT(*) as count
            FROM events
            GROUP BY closed
        """)
        closed_breakdown = {row[0]: row[1] for row in cursor.fetchall()}

        # Count by active status
        cursor.execute("""
            SELECT active, COUNT(*) as count
            FROM events
            GROUP BY active
        """)
        active_breakdown = {row[0]: row[1] for row in cursor.fetchall()}

        # Count by both
        cursor.execute("""
            SELECT active, closed, COUNT(*) as count
            FROM events
            GROUP BY active, closed
        """)
        combined = cursor.fetchall()

        total_events = sum(closed_breakdown.values())
        closed_count = closed_breakdown.get(1, 0)
        not_closed = closed_breakdown.get(0, 0)

        active_count = active_breakdown.get(1, 0)
        not_active = active_breakdown.get(0, 0)

        print(f"Total Events: {total_events:,}")
        print(f"\nBy 'closed' field:")
        print(f"  Closed (closed=1): {closed_count:,} ({closed_count/total_events*100:.1f}%)")
        print(f"  Not Closed (closed=0): {not_closed:,} ({not_closed/total_events*100:.1f}%)")

        print(f"\nBy 'active' field:")
        print(f"  Active (active=1): {active_count:,} ({active_count/total_events*100:.1f}%)")
        print(f"  Not Active (active=0): {not_active:,} ({not_active/total_events*100:.1f}%)")

        print(f"\nCombined breakdown:")
        for row in combined:
            active_str = "Active" if row[0] == 1 else "Inactive"
            closed_str = "Closed" if row[1] == 1 else "Open"
            print(f"  {active_str} + {closed_str}: {row[2]:,}")

        # Check recent fetches
        cursor.execute("""
            SELECT 
                DATE(fetched_at) as fetch_date,
                COUNT(*) as count,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed_count,
                SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_count
            FROM events
            WHERE fetched_at IS NOT NULL
            GROUP BY DATE(fetched_at)
            ORDER BY fetch_date DESC
            LIMIT 10
        """)
        recent_fetches = cursor.fetchall()

        print(f"\nRecent Fetch Activity:")
        for row in recent_fetches:
            print(f"  {row[0]}: {row[1]:,} events ({row[2]:,} closed, {row[3]:,} active)")

        return {
            'total_events': total_events,
            'closed_events': closed_count,
            'open_events': not_closed,
            'active_events': active_count,
            'inactive_events': not_active,
            'combined_breakdown': [dict(zip(['active', 'closed', 'count'], row)) for row in combined]
        }

    def compare_events_vs_markets(self) -> Dict:
        """Compare events count vs markets count"""
        cursor = self.conn.cursor()

        print("\n🔄 Comparing Events vs Markets...")
        print("=" * 60)

        # Count events
        cursor.execute("SELECT COUNT(*) FROM events")
        event_count = cursor.fetchone()[0]

        # Count markets
        cursor.execute("SELECT COUNT(*) FROM markets")
        market_count = cursor.fetchone()[0]

        # Count events with markets
        cursor.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM events e
            INNER JOIN markets m ON e.id = m.event_id
        """)
        events_with_markets = cursor.fetchone()[0]

        # Count events without markets
        events_without_markets = event_count - events_with_markets

        # Average markets per event
        cursor.execute("""
            SELECT AVG(market_count) as avg_markets
            FROM (
                SELECT e.id, COUNT(m.id) as market_count
                FROM events e
                LEFT JOIN markets m ON e.id = m.event_id
                GROUP BY e.id
            )
        """)
        avg_markets = cursor.fetchone()[0] or 0

        # Events with most markets
        cursor.execute("""
            SELECT e.id, e.title, COUNT(m.id) as market_count
            FROM events e
            LEFT JOIN markets m ON e.id = m.event_id
            GROUP BY e.id
            ORDER BY market_count DESC
            LIMIT 10
        """)
        events_most_markets = cursor.fetchall()

        print(f"Total Events: {event_count:,}")
        print(f"Total Markets: {market_count:,}")
        print(f"\n⚠️  RATIO: {event_count/market_count:.2f} events per market")
        print(f"   (Expected: Less than 1.0, typically 0.2-0.5)")

        if event_count > market_count:
            print(f"\n🚨 ISSUE DETECTED: More events than markets!")
            print(f"   This suggests duplicate events or closed events being fetched")

        print(f"\nEvents with Markets: {events_with_markets:,}")
        print(f"Events without Markets: {events_without_markets:,}")
        print(f"Average Markets per Event: {avg_markets:.2f}")

        print(f"\nTop Events by Market Count:")
        for row in events_most_markets:
            print(f"  {row[1][:50]}: {row[2]} markets")

        return {
            'event_count': event_count,
            'market_count': market_count,
            'ratio': event_count / market_count if market_count > 0 else 0,
            'events_with_markets': events_with_markets,
            'events_without_markets': events_without_markets,
            'avg_markets_per_event': avg_markets
        }

    def check_series_duplicates(self) -> Dict:
        """Check if series are creating duplicate events"""
        cursor = self.conn.cursor()

        print("\n🔍 Checking Series-Event Relationships...")
        print("=" * 60)

        # Check if event_ids in series_events might have duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM series_events
        """)
        series_events_count = cursor.fetchone()[0]

        # Count unique events referenced in series
        cursor.execute("""
            SELECT series_id, event_ids
            FROM series_events
        """)
        
        all_event_ids = []
        for row in cursor.fetchall():
            if row[1]:
                event_ids = json.loads(row[1])
                all_event_ids.extend(event_ids)

        unique_events_in_series = len(set(all_event_ids))
        total_references = len(all_event_ids)

        print(f"Series with events: {series_events_count:,}")
        print(f"Total event references in series: {total_references:,}")
        print(f"Unique events referenced: {unique_events_in_series:,}")

        if total_references > unique_events_in_series:
            print(f"⚠️  Same events appear in multiple series: {total_references - unique_events_in_series:,} duplicates")

        return {
            'series_count': series_events_count,
            'total_references': total_references,
            'unique_events': unique_events_in_series
        }

    def identify_problem_source(self) -> Dict:
        """Try to identify where duplicates are coming from"""
        cursor = self.conn.cursor()

        print("\n🔎 Identifying Problem Source...")
        print("=" * 60)

        # Check if process_all_events_detailed is creating duplicates
        cursor.execute("""
            SELECT 
                id,
                COUNT(*) as fetch_count,
                GROUP_CONCAT(DISTINCT fetched_at) as fetch_times
            FROM (
                SELECT id, fetched_at
                FROM events
            )
            GROUP BY id
            HAVING fetch_count > 1
            LIMIT 10
        """)
        
        refetched_events = cursor.fetchall()
        
        print(f"Events fetched multiple times: {len(refetched_events)}")
        if refetched_events:
            print("Note: This is NORMAL - events get updated, not duplicated")
            print("The INSERT OR REPLACE prevents actual duplicates")

        # Check for events that might have been added by series
        cursor.execute("""
            SELECT 
                e.id,
                e.title,
                e.closed,
                e.active,
                e.fetched_at
            FROM events e
            WHERE e.id NOT IN (SELECT DISTINCT event_id FROM markets)
            ORDER BY e.fetched_at DESC
            LIMIT 20
        """)
        events_without_markets = cursor.fetchall()

        print(f"\nRecent Events without Markets:")
        for row in events_without_markets[:10]:
            status = "CLOSED" if row[2] == 1 else "OPEN"
            active = "ACTIVE" if row[3] == 1 else "INACTIVE"
            print(f"  {row[0]}: {row[1][:40]} [{status}/{active}] - {row[4]}")

        return {
            'events_without_markets_sample': len(events_without_markets)
        }

    def generate_full_report(self) -> Dict:
        """Generate comprehensive events analysis report"""
        self.connect()

        print("\n" + "=" * 60)
        print("🔍 EVENTS TABLE ANALYSIS REPORT")
        print("=" * 60)

        report = {
            'timestamp': datetime.now().isoformat(),
            'duplicates': self.check_duplicate_events(),
            'closed_analysis': self.analyze_closed_events(),
            'events_vs_markets': self.compare_events_vs_markets(),
            'series_analysis': self.check_series_duplicates(),
            'problem_source': self.identify_problem_source()
        }

        # Summary and recommendations
        print("\n" + "=" * 60)
        print("📋 SUMMARY & RECOMMENDATIONS")
        print("=" * 60)

        if report['duplicates']['duplicate_ids'] > 0:
            print("❌ CRITICAL: Duplicate event IDs found - database corruption!")
        else:
            print("✅ No duplicate event IDs (good)")

        ratio = report['events_vs_markets']['ratio']
        if ratio > 1.0:
            print(f"⚠️  WARNING: Events/Markets ratio is {ratio:.2f}")
            print("   Recommendation: Check if closed events are being fetched")
            print("   Action: Verify FETCH_CLOSED_EVENTS config is False")
        else:
            print(f"✅ Events/Markets ratio is {ratio:.2f} (normal)")

        closed_pct = report['closed_analysis']['closed_events'] / report['closed_analysis']['total_events'] * 100
        if closed_pct > 50:
            print(f"⚠️  WARNING: {closed_pct:.1f}% of events are closed")
            print("   Recommendation: You may not want these in your database")
        else:
            print(f"✅ Only {closed_pct:.1f}% of events are closed")

        self.disconnect()

        return report


def main():
    """Run the events analysis"""
    import argparse

    parser = argparse.ArgumentParser(description='Analyze events table for issues')
    parser.add_argument('--db', default='polymarket_terminal.db', help='Database path')
    parser.add_argument('--output', help='Output JSON file')

    args = parser.parse_args()

    analyzer = EventsAnalyzer(db_path=args.db)
    report = analyzer.generate_full_report()

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\n✅ Report saved to: {args.output}")


if __name__ == "__main__":
    main()