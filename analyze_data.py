"""
Database Analysis Utility for Polymarket Terminal
Provides insights, statistics, and analysis of the database
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd


class DataAnalyzer:
    """Analyze database contents and provide insights"""

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

    def get_database_overview(self) -> Dict:
        """Get overall database statistics"""
        cursor = self.conn.cursor()

        overview = {
            'database_size': os.path.getsize(self.db_path) / (1024 * 1024),  # MB
            'tables': {}
        }

        # Get all tables
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        tables = [row[0] for row in cursor.fetchall()]

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            overview['tables'][table] = count

        overview['total_tables'] = len(tables)
        overview['total_records'] = sum(overview['tables'].values())

        return overview

    def analyze_events(self) -> Dict:
        """Analyze events data"""
        cursor = self.conn.cursor()

        analysis = {}

        # Total events
        cursor.execute("SELECT COUNT(*) FROM events")
        analysis['total_events'] = cursor.fetchone()[0]

        # Active vs closed
        cursor.execute("SELECT active, COUNT(*) as count FROM events GROUP BY active")
        status = {bool(row[0]): row[1] for row in cursor.fetchall()}
        analysis['active_events'] = status.get(True, 0)
        analysis['inactive_events'] = status.get(False, 0)

        # Closed vs open
        cursor.execute("SELECT closed, COUNT(*) as count FROM events GROUP BY closed")
        closed_status = {bool(row[0]): row[1] for row in cursor.fetchall()}
        analysis['closed_events'] = closed_status.get(True, 0)
        analysis['open_events'] = closed_status.get(False, 0)

        # Combined status
        cursor.execute("""
            SELECT 
                CASE WHEN active = 1 THEN 'Active' ELSE 'Inactive' END as active_status,
                CASE WHEN closed = 1 THEN 'Closed' ELSE 'Open' END as closed_status,
                COUNT(*) as count
            FROM events
            GROUP BY active, closed
        """)
        analysis['status_breakdown'] = {f"{row[0]}/{row[1]}": row[2] for row in cursor.fetchall()}

        # Top events by volume
        cursor.execute("""
            SELECT title, volume, liquidity, comment_count
            FROM events
            WHERE volume > 0
            ORDER BY volume DESC
            LIMIT 10
        """)
        analysis['top_events_by_volume'] = [dict(row) for row in cursor.fetchall()]

        # Events with most markets
        cursor.execute("""
            SELECT e.title, COUNT(m.id) as market_count, e.volume
            FROM events e
            LEFT JOIN markets m ON e.id = m.event_id
            GROUP BY e.id
            ORDER BY market_count DESC
            LIMIT 10
        """)
        analysis['events_with_most_markets'] = [dict(row) for row in cursor.fetchall()]

        # Events without markets
        cursor.execute("""
            SELECT COUNT(*)
            FROM events e
            WHERE NOT EXISTS (SELECT 1 FROM markets m WHERE m.event_id = e.id)
        """)
        analysis['events_without_markets'] = cursor.fetchone()[0]

        return analysis

    def analyze_markets(self) -> Dict:
        """Analyze markets data"""
        cursor = self.conn.cursor()

        analysis = {}

        # Total markets
        cursor.execute("SELECT COUNT(*) FROM markets")
        analysis['total_markets'] = cursor.fetchone()[0]

        # Active vs closed
        cursor.execute("SELECT active, COUNT(*) as count FROM markets GROUP BY active")
        status = {bool(row[0]): row[1] for row in cursor.fetchall()}
        analysis['active_markets'] = status.get(True, 0)
        analysis['closed_markets'] = status.get(False, 0)

        # Top markets by volume
        cursor.execute("""
            SELECT question, volume, liquidity, open_interest, last_trade_price
            FROM markets
            WHERE volume > 0
            ORDER BY volume DESC
            LIMIT 10
        """)
        analysis['top_markets_by_volume'] = [dict(row) for row in cursor.fetchall()]

        # Markets by volume range
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN volume < 1000 THEN '< $1K'
                    WHEN volume < 10000 THEN '$1K - $10K'
                    WHEN volume < 100000 THEN '$10K - $100K'
                    WHEN volume < 1000000 THEN '$100K - $1M'
                    ELSE '> $1M'
                END as volume_range,
                COUNT(*) as count
            FROM markets
            WHERE volume > 0
            GROUP BY volume_range
            ORDER BY MIN(volume)
        """)
        analysis['markets_by_volume_range'] = {row[0]: row[1] for row in cursor.fetchall()}

        return analysis

    def analyze_users(self) -> Dict:
        """Analyze users data"""
        cursor = self.conn.cursor()

        analysis = {}

        # Total users
        cursor.execute("SELECT COUNT(*) FROM users")
        analysis['total_users'] = cursor.fetchone()[0]

        # Whale users
        cursor.execute("SELECT COUNT(*) FROM users WHERE is_whale = 1")
        analysis['whale_users'] = cursor.fetchone()[0]

        # Users with usernames
        cursor.execute("SELECT COUNT(*) FROM users WHERE username IS NOT NULL")
        analysis['users_with_names'] = cursor.fetchone()[0]

        # Top users by value
        cursor.execute("""
            SELECT proxy_wallet, username, total_value, markets_traded
            FROM users
            WHERE total_value > 0
            ORDER BY total_value DESC
            LIMIT 20
        """)
        analysis['top_users_by_value'] = []
        for row in cursor.fetchall():
            user = dict(row)
            # Anonymize wallet addresses
            if user['proxy_wallet']:
                user['proxy_wallet'] = user['proxy_wallet'][:10] + '...'
            analysis['top_users_by_value'].append(user)

        # Users by portfolio size
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN total_value < 1000 THEN '< $1K'
                    WHEN total_value < 10000 THEN '$1K - $10K'
                    WHEN total_value < 100000 THEN '$10K - $100K'
                    WHEN total_value < 1000000 THEN '$100K - $1M'
                    ELSE '> $1M'
                END as portfolio_range,
                COUNT(*) as count
            FROM users
            WHERE total_value > 0
            GROUP BY portfolio_range
            ORDER BY MIN(total_value)
        """)
        analysis['users_by_portfolio_size'] = {row[0]: row[1] for row in cursor.fetchall()}

        return analysis

    def analyze_transactions(self) -> Dict:
        """Analyze transactions data"""
        cursor = self.conn.cursor()

        analysis = {}

        # Total transactions
        cursor.execute("SELECT COUNT(*) FROM transactions")
        analysis['total_transactions'] = cursor.fetchone()[0]

        # Whale transactions
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE is_whale = 1")
        analysis['whale_transactions'] = cursor.fetchone()[0]

        # Total volume
        cursor.execute("SELECT SUM(usdc_size) FROM transactions WHERE usdc_size > 0")
        total_volume = cursor.fetchone()[0]
        analysis['total_volume'] = total_volume if total_volume else 0

        # Buy vs Sell
        cursor.execute("""
            SELECT side, COUNT(*) as count, SUM(usdc_size) as volume
            FROM transactions
            GROUP BY side
        """)
        analysis['buy_sell_breakdown'] = {}
        for row in cursor.fetchall():
            if row[0]:
                analysis['buy_sell_breakdown'][row[0]] = {
                    'count': row[1],
                    'volume': row[2] if row[2] else 0
                }

        # Largest transactions
        cursor.execute("""
            SELECT proxy_wallet, usdc_size, side, outcome, time_created
            FROM transactions
            WHERE usdc_size > 0
            ORDER BY usdc_size DESC
            LIMIT 20
        """)
        analysis['largest_transactions'] = []
        for row in cursor.fetchall():
            tx = dict(row)
            # Anonymize wallet
            if tx['proxy_wallet']:
                tx['proxy_wallet'] = tx['proxy_wallet'][:10] + '...'
            analysis['largest_transactions'].append(tx)

        # Transactions by size range
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN usdc_size < 100 THEN '< $100'
                    WHEN usdc_size < 500 THEN '$100 - $500'
                    WHEN usdc_size < 1000 THEN '$500 - $1K'
                    WHEN usdc_size < 10000 THEN '$1K - $10K'
                    WHEN usdc_size < 100000 THEN '$10K - $100K'
                    ELSE '> $100K'
                END as size_range,
                COUNT(*) as count
            FROM transactions
            WHERE usdc_size > 0
            GROUP BY size_range
            ORDER BY MIN(usdc_size)
        """)
        analysis['transactions_by_size'] = {row[0]: row[1] for row in cursor.fetchall()}

        return analysis

    def analyze_market_holders(self) -> Dict:
        """Analyze market holders data"""
        cursor = self.conn.cursor()

        analysis = {}

        # Total holder records
        cursor.execute("SELECT COUNT(*) FROM market_holders")
        analysis['total_holder_records'] = cursor.fetchone()[0]

        # Unique holders
        cursor.execute("SELECT COUNT(DISTINCT proxy_wallet) FROM market_holders")
        analysis['unique_holders'] = cursor.fetchone()[0]

        # Markets with holders
        cursor.execute("SELECT COUNT(DISTINCT market_id) FROM market_holders")
        analysis['markets_with_holders'] = cursor.fetchone()[0]

        # Top holders by total positions
        cursor.execute("""
            SELECT proxy_wallet, username, COUNT(*) as position_count, SUM(amount) as total_amount
            FROM market_holders
            GROUP BY proxy_wallet
            ORDER BY total_amount DESC
            LIMIT 20
        """)
        analysis['top_holders'] = []
        for row in cursor.fetchall():
            holder = dict(row)
            # Anonymize wallet
            if holder['proxy_wallet']:
                holder['proxy_wallet'] = holder['proxy_wallet'][:10] + '...'
            analysis['top_holders'].append(holder)

        return analysis

    def generate_full_report(self, output_file: str = None) -> Dict:
        """Generate a comprehensive analysis report"""
        self.connect()

        print("📊 Generating Database Analysis Report...")
        print("=" * 60)

        report = {
            'generated_at': datetime.now().isoformat(),
            'database_path': self.db_path,
            'overview': self.get_database_overview(),
            'events_analysis': self.analyze_events(),
            'markets_analysis': self.analyze_markets(),
            'users_analysis': self.analyze_users(),
            'transactions_analysis': self.analyze_transactions(),
            'holders_analysis': self.analyze_market_holders()
        }

        self.disconnect()

        # Print summary
        print(f"📈 Database Overview:")
        print(f"   Size: {report['overview']['database_size']:.2f} MB")
        print(f"   Tables: {report['overview']['total_tables']}")
        print(f"   Total Records: {report['overview']['total_records']:,}")

        print(f"\n📊 Key Metrics:")
        print(
            f"   Events: {report['events_analysis']['total_events']:,} ({report['events_analysis']['active_events']:,} active, {report['events_analysis']['closed_events']:,} closed)")
        print(
            f"   Markets: {report['markets_analysis']['total_markets']:,} ({report['markets_analysis']['active_markets']:,} active)")
        print(
            f"   Users: {report['users_analysis']['total_users']:,} ({report['users_analysis']['whale_users']:,} whales)")
        print(f"   Transactions: {report['transactions_analysis']['total_transactions']:,}")
        print(f"   Total Volume: ${report['transactions_analysis']['total_volume']:,.2f}")

        # Add warnings if needed
        events_analysis = report['events_analysis']
        if events_analysis['closed_events'] > events_analysis['open_events']:
            print(f"\n⚠️  WARNING: More closed events ({events_analysis['closed_events']:,}) than open events ({events_analysis['open_events']:,})")
            print("   Check if FETCH_CLOSED_EVENTS is enabled in config")
        
        if events_analysis.get('events_without_markets', 0) > 0:
            print(f"\n⚠️  INFO: {events_analysis['events_without_markets']:,} events have no markets")
            print("   This may be normal for recently added events")

        # Save to file if requested
        if output_file:
            Path(os.path.dirname(output_file)).mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
            print(f"\n✅ Report saved to: {output_file}")

        print("=" * 60)

        return report


def main():
    """Main function to run the analysis"""
    import argparse

    parser = argparse.ArgumentParser(description='Analyze Polymarket database')
    parser.add_argument('--db', default='polymarket_terminal.db', help='Database path')
    parser.add_argument('--output', default='backend/data/analysis_report.json', help='Output file for report')

    args = parser.parse_args()

    # Create analyzer
    analyzer = DataAnalyzer(db_path=args.db)

    # Generate report
    analyzer.generate_full_report(output_file=args.output)


if __name__ == "__main__":
    main()