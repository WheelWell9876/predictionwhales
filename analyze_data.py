#!/usr/bin/env python3
"""
Polymarket Data Analyzer
Comprehensive analysis of the Polymarket terminal database
"""

import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

def connect_db(db_path: str) -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def analyze_events(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze events data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Total events
    cursor.execute("SELECT COUNT(*) as count FROM events")
    analysis['total'] = cursor.fetchone()['count']
    
    # Active vs Closed
    cursor.execute("SELECT COUNT(*) as count FROM events WHERE closed = 0")
    analysis['active'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM events WHERE closed = 1")
    analysis['closed'] = cursor.fetchone()['count']
    
    # Top events by volume
    cursor.execute("""
        SELECT title, volume, liquidity, market_count
        FROM events
        WHERE closed = 0
        ORDER BY volume DESC
        LIMIT 10
    """)
    analysis['top_by_volume'] = [dict(row) for row in cursor.fetchall()]
    
    # Recent events
    cursor.execute("""
        SELECT title, created_at, volume
        FROM events
        ORDER BY created_at DESC
        LIMIT 10
    """)
    analysis['recent'] = [dict(row) for row in cursor.fetchall()]
    
    return analysis

def analyze_markets(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze markets data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Total markets
    cursor.execute("SELECT COUNT(*) as count FROM markets")
    analysis['total'] = cursor.fetchone()['count']
    
    # Active markets
    cursor.execute("SELECT COUNT(*) as count FROM markets WHERE active = 1")
    analysis['active'] = cursor.fetchone()['count']
    
    # Markets by outcome
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN outcome_prices LIKE '%"outcome":"Yes"%' THEN 1 ELSE 0 END) as binary,
            SUM(CASE WHEN outcome_prices NOT LIKE '%"outcome":"Yes"%' THEN 1 ELSE 0 END) as multiple
        FROM markets
    """)
    row = cursor.fetchone()
    analysis['binary_markets'] = row['binary']
    analysis['multiple_outcome_markets'] = row['multiple']
    
    # Top markets by volume
    cursor.execute("""
        SELECT question, volume, liquidity, outcome_prices
        FROM markets
        WHERE active = 1
        ORDER BY volume DESC
        LIMIT 10
    """)
    analysis['top_by_volume'] = [dict(row) for row in cursor.fetchall()]
    
    return analysis

def analyze_users(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze users data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Total users
    cursor.execute("SELECT COUNT(*) as count FROM users")
    analysis['total'] = cursor.fetchone()['count']
    
    # Whale users
    cursor.execute("SELECT COUNT(*) as count FROM users WHERE is_whale = 1")
    analysis['whales'] = cursor.fetchone()['count']
    
    # Top users by value
    cursor.execute("""
        SELECT username, pseudonym, total_value
        FROM users
        WHERE is_whale = 1
        ORDER BY total_value DESC
        LIMIT 10
    """)
    analysis['top_whales'] = [dict(row) for row in cursor.fetchall()]
    
    # User activity stats
    cursor.execute("SELECT COUNT(*) as count FROM user_activity")
    analysis['total_activities'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM user_trades")
    analysis['total_trades'] = cursor.fetchone()['count']
    
    return analysis

def analyze_positions(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze positions data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Current positions
    cursor.execute("SELECT COUNT(*) as count FROM user_positions_current")
    analysis['current_positions'] = cursor.fetchone()['count']
    
    # Closed positions
    cursor.execute("SELECT COUNT(*) as count FROM user_positions_closed")
    analysis['closed_positions'] = cursor.fetchone()['count']
    
    # Top current positions by value
    cursor.execute("""
        SELECT proxy_wallet, title, current_value, percent_pnl
        FROM user_positions_current
        WHERE current_value > 0
        ORDER BY current_value DESC
        LIMIT 10
    """)
    analysis['top_current_positions'] = [dict(row) for row in cursor.fetchall()]
    
    # Top winning closed positions
    cursor.execute("""
        SELECT proxy_wallet, title, realized_pnl
        FROM user_positions_closed
        WHERE realized_pnl > 0
        ORDER BY realized_pnl DESC
        LIMIT 10
    """)
    analysis['top_winners'] = [dict(row) for row in cursor.fetchall()]
    
    # Top losing closed positions
    cursor.execute("""
        SELECT proxy_wallet, title, realized_pnl
        FROM user_positions_closed
        WHERE realized_pnl < 0
        ORDER BY realized_pnl ASC
        LIMIT 10
    """)
    analysis['top_losers'] = [dict(row) for row in cursor.fetchall()]
    
    return analysis

def analyze_transactions(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze transactions data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Total transactions
    cursor.execute("SELECT COUNT(*) as count FROM transactions")
    analysis['total_transactions'] = cursor.fetchone()['count']
    
    # Whale transactions
    cursor.execute("SELECT COUNT(*) as count FROM transactions WHERE is_whale = 1")
    analysis['whale_transactions'] = cursor.fetchone()['count']
    
    # Transaction volume
    cursor.execute("""
        SELECT 
            SUM(usdc_size) as total_volume,
            AVG(usdc_size) as avg_size,
            MAX(usdc_size) as max_size
        FROM transactions
    """)
    row = cursor.fetchone()
    analysis['total_volume'] = row['total_volume'] or 0
    analysis['average_size'] = row['avg_size'] or 0
    analysis['max_size'] = row['max_size'] or 0
    
    # Top transactions
    cursor.execute("""
        SELECT proxy_wallet, usdc_size, side, timestamp
        FROM transactions
        ORDER BY usdc_size DESC
        LIMIT 10
    """)
    analysis['top_transactions'] = [dict(row) for row in cursor.fetchall()]
    
    return analysis

def analyze_comments(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze comments data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Total comments
    cursor.execute("SELECT COUNT(*) as count FROM comments")
    analysis['total'] = cursor.fetchone()['count']
    
    # Comments with reactions
    cursor.execute("SELECT COUNT(DISTINCT comment_id) as count FROM comment_reactions")
    analysis['comments_with_reactions'] = cursor.fetchone()['count']
    
    # Total reactions
    cursor.execute("SELECT COUNT(*) as count FROM comment_reactions")
    analysis['total_reactions'] = cursor.fetchone()['count']
    
    # Most commented events
    cursor.execute("""
        SELECT event_id, COUNT(*) as comment_count
        FROM comments
        GROUP BY event_id
        ORDER BY comment_count DESC
        LIMIT 10
    """)
    analysis['most_commented_events'] = [dict(row) for row in cursor.fetchall()]
    
    return analysis

def analyze_series_and_tags(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze series and tags data"""
    cursor = conn.cursor()
    
    analysis = {}
    
    # Series stats
    cursor.execute("SELECT COUNT(*) as count FROM series")
    analysis['total_series'] = cursor.fetchone()['count']
    
    cursor.execute("""
        SELECT title, volume, liquidity
        FROM series
        ORDER BY volume DESC
        LIMIT 5
    """)
    analysis['top_series'] = [dict(row) for row in cursor.fetchall()]
    
    # Tags stats
    cursor.execute("SELECT COUNT(*) as count FROM tags")
    analysis['total_tags'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM tag_relationships")
    analysis['tag_relationships'] = cursor.fetchone()['count']
    
    return analysis

def generate_summary(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate overall summary statistics"""
    summary = {
        'generated_at': datetime.now().isoformat(),
        'database_health': 'healthy',
        'key_metrics': {
            'total_events': analysis['events']['total'],
            'active_events': analysis['events']['active'],
            'total_markets': analysis['markets']['total'],
            'total_users': analysis['users']['total'],
            'whale_users': analysis['users']['whales'],
            'current_positions': analysis['positions']['current_positions'],
            'total_transactions': analysis['transactions']['total_transactions'],
            'total_comments': analysis['comments']['total']
        },
        'data_quality': {
            'events_with_markets': len([e for e in analysis['events']['top_by_volume'] if e.get('market_count', 0) > 0]),
            'markets_active_ratio': analysis['markets']['active'] / max(analysis['markets']['total'], 1),
            'whale_user_ratio': analysis['users']['whales'] / max(analysis['users']['total'], 1),
            'whale_transaction_ratio': analysis['transactions']['whale_transactions'] / max(analysis['transactions']['total_transactions'], 1)
        }
    }
    
    # Check for data issues
    issues = []
    if analysis['events']['closed'] > 0:
        issues.append(f"{analysis['events']['closed']} closed events in database")
    if analysis['markets']['total'] == 0:
        issues.append("No markets data")
    if analysis['users']['whales'] == 0:
        issues.append("No whale users identified")
    
    if issues:
        summary['database_health'] = 'needs_attention'
        summary['issues'] = issues
    
    return summary

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Analyze Polymarket Terminal Database')
    parser.add_argument('--db', type=str, default='polymarket_terminal.db',
                       help='Path to database file')
    parser.add_argument('--output', type=str, default='report.json',
                       help='Output file for analysis report')
    parser.add_argument('--print', action='store_true',
                       help='Print summary to console')
    
    args = parser.parse_args()
    
    # Check if database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Database not found: {args.db}")
        sys.exit(1)
    
    print("\n" + "=" * 80)
    print("📊 POLYMARKET DATABASE ANALYZER")
    print("=" * 80)
    print(f"📁 Database: {args.db}")
    print(f"📄 Output: {args.output}")
    
    # Connect to database
    conn = connect_db(args.db)
    
    # Perform analysis
    print("\n🔍 Analyzing data...")
    
    analysis = {
        'events': analyze_events(conn),
        'markets': analyze_markets(conn),
        'users': analyze_users(conn),
        'positions': analyze_positions(conn),
        'transactions': analyze_transactions(conn),
        'comments': analyze_comments(conn),
        'series_tags': analyze_series_and_tags(conn),
        'summary': {}
    }
    
    # Generate summary
    analysis['summary'] = generate_summary(analysis)
    
    # Close connection
    conn.close()
    
    # Save report
    with open(args.output, 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    
    print(f"✅ Analysis complete! Report saved to {args.output}")
    
    # Print summary if requested
    if args.print:
        print("\n" + "=" * 80)
        print("📈 SUMMARY")
        print("=" * 80)
        
        summary = analysis['summary']
        
        print("\n📊 Key Metrics:")
        for key, value in summary['key_metrics'].items():
            print(f"   {key.replace('_', ' ').title():<25} {value:>10,}")
        
        print("\n📈 Data Quality:")
        for key, value in summary['data_quality'].items():
            if isinstance(value, float):
                print(f"   {key.replace('_', ' ').title():<25} {value:>10.2%}")
            else:
                print(f"   {key.replace('_', ' ').title():<25} {value:>10}")
        
        if 'issues' in summary:
            print("\n⚠️ Issues Found:")
            for issue in summary['issues']:
                print(f"   • {issue}")
        
        print(f"\n💾 Database Health: {summary['database_health'].upper()}")
    
    print("\n✨ Done!")

if __name__ == "__main__":
    main()