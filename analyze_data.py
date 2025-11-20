#!/usr/bin/env python3
"""
Polymarket Data Analyzer - Enhanced Version
Comprehensive analysis of the Polymarket terminal database with detailed column analysis and sample data
"""

import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

def connect_db(db_path: str) -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_table_info(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Get detailed information about a table including columns and data types"""
    cursor = conn.cursor()
    
    # Get column information
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = []
    for row in cursor.fetchall():
        columns.append({
            'name': row[1],
            'type': row[2],
            'nullable': not row[3],
            'default': row[4],
            'primary_key': row[5] == 1
        })
    
    return columns

def get_sample_rows(conn: sqlite3.Connection, table_name: str, limit: int = 5) -> List[Dict]:
    """Get sample rows from a table"""
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        rows = []
        for row in cursor.fetchall():
            rows.append(dict(row))
        return rows
    except Exception as e:
        return []

def analyze_column_data(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Analyze the data in each column of a table"""
    cursor = conn.cursor()
    analysis = {}
    
    # Get columns
    columns_info = get_table_info(conn, table_name)
    
    for col in columns_info:
        col_name = col['name']
        col_analysis = {
            'type': col['type'],
            'nullable': col['nullable'],
            'primary_key': col['primary_key']
        }
        
        try:
            # Count non-null values
            cursor.execute(f"SELECT COUNT(*) as total, COUNT({col_name}) as non_null FROM {table_name}")
            result = cursor.fetchone()
            col_analysis['total_rows'] = result['total']
            col_analysis['non_null_count'] = result['non_null']
            col_analysis['null_count'] = result['total'] - result['non_null']
            col_analysis['null_percentage'] = (col_analysis['null_count'] / max(result['total'], 1)) * 100
            
            # For numeric columns, get stats
            if 'INT' in col['type'].upper() or 'REAL' in col['type'].upper() or 'NUMERIC' in col['type'].upper():
                cursor.execute(f"""
                    SELECT 
                        MIN({col_name}) as min_val,
                        MAX({col_name}) as max_val,
                        AVG({col_name}) as avg_val,
                        SUM({col_name}) as sum_val
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                """)
                stats = cursor.fetchone()
                if stats:
                    col_analysis['min'] = stats['min_val']
                    col_analysis['max'] = stats['max_val']
                    col_analysis['avg'] = stats['avg_val']
                    col_analysis['sum'] = stats['sum_val']
            
            # For text columns, get unique count
            elif 'TEXT' in col['type'].upper() or 'CHAR' in col['type'].upper():
                cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) as unique_count FROM {table_name}")
                col_analysis['unique_values'] = cursor.fetchone()['unique_count']
                
                # Get sample values for text columns
                cursor.execute(f"""
                    SELECT DISTINCT {col_name} 
                    FROM {table_name} 
                    WHERE {col_name} IS NOT NULL 
                    LIMIT 5
                """)
                col_analysis['sample_values'] = [row[0] for row in cursor.fetchall()]
        
        except Exception as e:
            col_analysis['error'] = str(e)
        
        analysis[col_name] = col_analysis
    
    return analysis

def analyze_table_comprehensive(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Comprehensive analysis of a single table"""
    cursor = conn.cursor()
    
    analysis = {
        'table_name': table_name,
        'row_count': 0,
        'columns': {},
        'column_analysis': {},
        'sample_data': [],
        'indexes': []
    }
    
    # Get row count
    try:
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        analysis['row_count'] = cursor.fetchone()['count']
    except:
        analysis['row_count'] = 0
    
    # Get column information
    analysis['columns'] = get_table_info(conn, table_name)
    
    # Get column analysis (detailed stats)
    if analysis['row_count'] > 0:
        analysis['column_analysis'] = analyze_column_data(conn, table_name)
    
    # Get sample data
    analysis['sample_data'] = get_sample_rows(conn, table_name, 5)
    
    # Get indexes
    cursor.execute(f"PRAGMA index_list({table_name})")
    for row in cursor.fetchall():
        analysis['indexes'].append({
            'name': row[1],
            'unique': row[2] == 1
        })
    
    return analysis

def analyze_events_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of events table"""
    analysis = analyze_table_comprehensive(conn, 'events')
    
    cursor = conn.cursor()
    
    # Additional event-specific analysis
    extra_analysis = {}
    
    # Active vs Closed breakdown
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed_count,
            SUM(CASE WHEN archived = 1 THEN 1 ELSE 0 END) as archived_count,
            SUM(CASE WHEN featured = 1 THEN 1 ELSE 0 END) as featured_count,
            SUM(CASE WHEN restricted = 1 THEN 1 ELSE 0 END) as restricted_count
        FROM events
    """)
    status_row = cursor.fetchone()
    extra_analysis['status_breakdown'] = dict(status_row) if status_row else {}
    
    # Volume statistics
    cursor.execute("""
        SELECT 
            SUM(volume) as total_volume,
            AVG(volume) as avg_volume,
            MAX(volume) as max_volume,
            MIN(volume) as min_volume,
            SUM(volume_24hr) as total_volume_24hr,
            AVG(liquidity) as avg_liquidity
        FROM events
        WHERE active = 1
    """)
    volume_row = cursor.fetchone()
    extra_analysis['volume_stats'] = dict(volume_row) if volume_row else {}
    
    # Top events by volume
    cursor.execute("""
        SELECT id, ticker, slug, title, volume, liquidity, competitive
        FROM events
        WHERE active = 1
        ORDER BY volume DESC
        LIMIT 10
    """)
    extra_analysis['top_by_volume'] = [dict(row) for row in cursor.fetchall()]
    
    # Date range analysis
    cursor.execute("""
        SELECT 
            MIN(created_at) as earliest_created,
            MAX(created_at) as latest_created,
            MIN(start_date) as earliest_start,
            MAX(end_date) as latest_end
        FROM events
    """)
    date_row = cursor.fetchone()
    extra_analysis['date_range'] = dict(date_row) if date_row else {}
    
    analysis['event_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_all_tables(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze all tables in the database"""
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    analysis = {}
    
    for table in tables:
        print(f"  Analyzing {table}...")
        if table == 'events':
            # Detailed analysis for events
            analysis[table] = analyze_events_detailed(conn)
        else:
            # Basic analysis for other tables
            analysis[table] = {
                'table_name': table,
                'row_count': 0,
                'columns': get_table_info(conn, table),
                'sample_data': get_sample_rows(conn, table, 5)
            }
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                analysis[table]['row_count'] = cursor.fetchone()['count']
            except:
                pass
    
    return analysis

def generate_summary(tables_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate overall summary statistics"""
    summary = {
        'generated_at': datetime.now().isoformat(),
        'database_health': 'healthy',
        'tables_overview': {},
        'data_quality_metrics': {}
    }
    
    # Tables overview
    for table_name, table_data in tables_analysis.items():
        summary['tables_overview'][table_name] = {
            'row_count': table_data.get('row_count', 0),
            'column_count': len(table_data.get('columns', [])),
            'has_data': table_data.get('row_count', 0) > 0
        }
    
    # Calculate data quality metrics
    total_tables = len(tables_analysis)
    tables_with_data = sum(1 for t in tables_analysis.values() if t.get('row_count', 0) > 0)
    
    summary['data_quality_metrics'] = {
        'total_tables': total_tables,
        'tables_with_data': tables_with_data,
        'empty_tables': total_tables - tables_with_data,
        'data_coverage_percentage': (tables_with_data / max(total_tables, 1)) * 100
    }
    
    # Check for specific issues
    issues = []
    
    # Check events table
    if 'events' in tables_analysis:
        events_data = tables_analysis['events']
        if events_data.get('row_count', 0) == 0:
            issues.append("Events table is empty")
        elif 'event_specific_analysis' in events_data:
            status = events_data['event_specific_analysis'].get('status_breakdown', {})
            if status.get('closed_count', 0) > 0:
                issues.append(f"{status['closed_count']} closed events found in database")
    
    # Check other critical tables
    critical_tables = ['markets', 'users', 'tags']
    for table in critical_tables:
        if table in tables_analysis and tables_analysis[table].get('row_count', 0) == 0:
            issues.append(f"{table.capitalize()} table is empty")
    
    if issues:
        summary['database_health'] = 'needs_attention'
        summary['issues'] = issues
    else:
        summary['database_health'] = 'healthy'
    
    return summary

def print_summary(analysis: Dict[str, Any]):
    """Print a formatted summary to console"""
    summary = analysis.get('summary', {})
    
    print("\n" + "=" * 80)
    print("📊 DATABASE ANALYSIS SUMMARY")
    print("=" * 80)
    
    print(f"\n⏰ Generated: {summary.get('generated_at', 'Unknown')}")
    print(f"💾 Database Health: {summary.get('database_health', 'Unknown').upper()}")
    
    print("\n📋 Tables Overview:")
    for table, info in summary.get('tables_overview', {}).items():
        status = "✅" if info['has_data'] else "⚠️"
        print(f"  {status} {table:<30} {info['row_count']:>10,} rows, {info['column_count']:>3} columns")
    
    print("\n📈 Data Quality Metrics:")
    metrics = summary.get('data_quality_metrics', {})
    print(f"  Total Tables:        {metrics.get('total_tables', 0)}")
    print(f"  Tables with Data:    {metrics.get('tables_with_data', 0)}")
    print(f"  Empty Tables:        {metrics.get('empty_tables', 0)}")
    print(f"  Data Coverage:       {metrics.get('data_coverage_percentage', 0):.1f}%")
    
    if 'issues' in summary:
        print("\n⚠️ Issues Found:")
        for issue in summary['issues']:
            print(f"  • {issue}")
    
    # Events specific summary if available
    if 'events' in analysis and 'event_specific_analysis' in analysis['events']:
        event_analysis = analysis['events']['event_specific_analysis']
        
        print("\n📊 Events Analysis:")
        status = event_analysis.get('status_breakdown', {})
        print(f"  Active Events:       {status.get('active_count', 0):>10,}")
        print(f"  Closed Events:       {status.get('closed_count', 0):>10,}")
        print(f"  Featured Events:     {status.get('featured_count', 0):>10,}")
        
        volume = event_analysis.get('volume_stats', {})
        if volume.get('total_volume'):
            print(f"\n  Total Volume:        ${volume.get('total_volume', 0):>10,.2f}")
            print(f"  Average Volume:      ${volume.get('avg_volume', 0):>10,.2f}")
            print(f"  Max Volume:          ${volume.get('max_volume', 0):>10,.2f}")

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
    print("🔊 POLYMARKET DATABASE ANALYZER - ENHANCED")
    print("=" * 80)
    print(f"📁 Database: {args.db}")
    print(f"📄 Output: {args.output}")
    
    # Connect to database
    conn = connect_db(args.db)
    
    # Perform analysis
    print("\n🔍 Analyzing database structure and data...")
    
    # Analyze all tables
    tables_analysis = analyze_all_tables(conn)
    
    # Generate summary
    summary = generate_summary(tables_analysis)
    
    # Create final report
    report = {
        'summary': summary,
        'tables': tables_analysis
    }
    
    # Close connection
    conn.close()
    
    # Save report
    with open(args.output, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n✅ Analysis complete! Report saved to {args.output}")
    
    # Print summary if requested or by default
    if args.print or True:  # Always print summary
        print_summary(report)
    
    print("\n✨ Done!")

if __name__ == "__main__":
    main()