#!/usr/bin/env python3
"""
Polymarket Data Analyzer - Enhanced Version
Comprehensive analysis of the Polymarket terminal database with detailed analysis for all tables
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
    
    # Events with markets count
    cursor.execute("""
        SELECT COUNT(DISTINCT event_id) as events_with_markets
        FROM markets
    """)
    extra_analysis['events_with_markets'] = cursor.fetchone()['events_with_markets']
    
    # Events with tags count
    cursor.execute("""
        SELECT COUNT(DISTINCT event_id) as events_with_tags
        FROM event_tags
    """)
    extra_analysis['events_with_tags'] = cursor.fetchone()['events_with_tags']
    
    analysis['event_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_markets_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of markets table"""
    analysis = analyze_table_comprehensive(conn, 'markets')
    
    cursor = conn.cursor()
    
    # Additional market-specific analysis
    extra_analysis = {}
    
    # Status breakdown
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed_count,
            SUM(CASE WHEN archived = 1 THEN 1 ELSE 0 END) as archived_count,
            SUM(CASE WHEN featured = 1 THEN 1 ELSE 0 END) as featured_count,
            SUM(CASE WHEN restricted = 1 THEN 1 ELSE 0 END) as restricted_count,
            SUM(CASE WHEN ready = 1 THEN 1 ELSE 0 END) as ready_count,
            SUM(CASE WHEN funded = 1 THEN 1 ELSE 0 END) as funded_count
        FROM markets
    """)
    status_row = cursor.fetchone()
    extra_analysis['status_breakdown'] = dict(status_row) if status_row else {}
    
    # Volume and liquidity statistics
    cursor.execute("""
        SELECT 
            SUM(volume_num) as total_volume,
            AVG(volume_num) as avg_volume,
            MAX(volume_num) as max_volume,
            MIN(volume_num) as min_volume,
            SUM(volume_24hr) as total_volume_24hr,
            AVG(liquidity_num) as avg_liquidity,
            SUM(liquidity_num) as total_liquidity,
            MAX(liquidity_num) as max_liquidity
        FROM markets
        WHERE active = 1
    """)
    volume_row = cursor.fetchone()
    extra_analysis['volume_stats'] = dict(volume_row) if volume_row else {}
    
    # Top markets by volume
    cursor.execute("""
        SELECT id, question, slug, volume_num, liquidity_num, last_trade_price, spread
        FROM markets
        WHERE active = 1
        ORDER BY volume_num DESC
        LIMIT 10
    """)
    extra_analysis['top_by_volume'] = [dict(row) for row in cursor.fetchall()]
    
    # Markets by event
    cursor.execute("""
        SELECT event_id, COUNT(*) as market_count, SUM(volume_num) as total_volume
        FROM markets
        GROUP BY event_id
        ORDER BY market_count DESC
        LIMIT 10
    """)
    extra_analysis['top_events_by_market_count'] = [dict(row) for row in cursor.fetchall()]
    
    # Order book enabled markets
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN enable_order_book = 1 THEN 1 ELSE 0 END) as order_book_enabled,
            SUM(CASE WHEN accepting_orders = 1 THEN 1 ELSE 0 END) as accepting_orders
        FROM markets
    """)
    order_stats = cursor.fetchone()
    extra_analysis['order_book_stats'] = dict(order_stats) if order_stats else {}
    
    # Markets with tags
    cursor.execute("""
        SELECT COUNT(DISTINCT market_id) as markets_with_tags
        FROM market_tags
    """)
    extra_analysis['markets_with_tags'] = cursor.fetchone()['markets_with_tags']
    
    analysis['market_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_series_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of series table"""
    analysis = analyze_table_comprehensive(conn, 'series')
    
    cursor = conn.cursor()
    
    # Additional series-specific analysis
    extra_analysis = {}
    
    # Status breakdown
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed_count,
            SUM(CASE WHEN archived = 1 THEN 1 ELSE 0 END) as archived_count,
            SUM(CASE WHEN featured = 1 THEN 1 ELSE 0 END) as featured_count,
            SUM(CASE WHEN restricted = 1 THEN 1 ELSE 0 END) as restricted_count
        FROM series
    """)
    status_row = cursor.fetchone()
    extra_analysis['status_breakdown'] = dict(status_row) if status_row else {}
    
    # Volume and liquidity statistics
    cursor.execute("""
        SELECT 
            SUM(volume) as total_volume,
            AVG(volume) as avg_volume,
            MAX(volume) as max_volume,
            SUM(liquidity) as total_liquidity,
            AVG(liquidity) as avg_liquidity
        FROM series
        WHERE active = 1
    """)
    volume_row = cursor.fetchone()
    extra_analysis['volume_stats'] = dict(volume_row) if volume_row else {}
    
    # Top series by volume
    cursor.execute("""
        SELECT id, ticker, slug, title, volume, liquidity
        FROM series
        ORDER BY volume DESC
        LIMIT 10
    """)
    extra_analysis['top_by_volume'] = [dict(row) for row in cursor.fetchall()]
    
    # Series with events
    cursor.execute("""
        SELECT series_id, COUNT(*) as event_count
        FROM series_events
        GROUP BY series_id
        ORDER BY event_count DESC
        LIMIT 10
    """)
    extra_analysis['top_series_by_event_count'] = [dict(row) for row in cursor.fetchall()]
    
    # Series with tags
    cursor.execute("""
        SELECT COUNT(DISTINCT series_id) as series_with_tags
        FROM series_tags
    """)
    extra_analysis['series_with_tags'] = cursor.fetchone()['series_with_tags']
    
    # Series with categories
    cursor.execute("""
        SELECT COUNT(DISTINCT series_id) as series_with_categories
        FROM series_categories
    """)
    extra_analysis['series_with_categories'] = cursor.fetchone()['series_with_categories']
    
    analysis['series_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_tags_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of tags table"""
    analysis = analyze_table_comprehensive(conn, 'tags')
    
    cursor = conn.cursor()
    
    # Additional tag-specific analysis
    extra_analysis = {}
    
    # Tag usage statistics
    cursor.execute("""
        SELECT 
            (SELECT COUNT(DISTINCT tag_id) FROM event_tags) as tags_on_events,
            (SELECT COUNT(DISTINCT tag_id) FROM market_tags) as tags_on_markets,
            (SELECT COUNT(DISTINCT tag_id) FROM series_tags) as tags_on_series
    """)
    usage_row = cursor.fetchone()
    extra_analysis['usage_stats'] = dict(usage_row) if usage_row else {}
    
    # Most used tags on events
    cursor.execute("""
        SELECT t.id, t.label, t.slug, COUNT(et.event_id) as usage_count
        FROM tags t
        JOIN event_tags et ON t.id = et.tag_id
        GROUP BY t.id, t.label, t.slug
        ORDER BY usage_count DESC
        LIMIT 10
    """)
    extra_analysis['top_tags_on_events'] = [dict(row) for row in cursor.fetchall()]
    
    # Most used tags on markets
    cursor.execute("""
        SELECT t.id, t.label, t.slug, COUNT(mt.market_id) as usage_count
        FROM tags t
        JOIN market_tags mt ON t.id = mt.tag_id
        GROUP BY t.id, t.label, t.slug
        ORDER BY usage_count DESC
        LIMIT 10
    """)
    extra_analysis['top_tags_on_markets'] = [dict(row) for row in cursor.fetchall()]
    
    # Tag relationships
    cursor.execute("""
        SELECT COUNT(*) as total_relationships,
               COUNT(DISTINCT tag_id) as tags_with_relationships
        FROM tag_relationships
    """)
    rel_stats = cursor.fetchone()
    extra_analysis['relationship_stats'] = dict(rel_stats) if rel_stats else {}
    
    # Carousel tags
    cursor.execute("""
        SELECT COUNT(*) as carousel_tags
        FROM tags
        WHERE is_carousel = 1
    """)
    extra_analysis['carousel_tags'] = cursor.fetchone()['carousel_tags']
    
    analysis['tag_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_categories_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of categories table"""
    analysis = analyze_table_comprehensive(conn, 'categories')
    
    cursor = conn.cursor()
    
    # Additional category-specific analysis
    extra_analysis = {}
    
    # Category usage
    cursor.execute("""
        SELECT 
            (SELECT COUNT(DISTINCT category_id) FROM event_categories) as categories_on_events,
            (SELECT COUNT(DISTINCT category_id) FROM market_categories) as categories_on_markets,
            (SELECT COUNT(DISTINCT category_id) FROM series_categories) as categories_on_series
    """)
    usage_row = cursor.fetchone()
    extra_analysis['usage_stats'] = dict(usage_row) if usage_row else {}
    
    # Top categories by event count
    cursor.execute("""
        SELECT c.id, c.label, c.slug, COUNT(ec.event_id) as event_count
        FROM categories c
        JOIN event_categories ec ON c.id = ec.category_id
        GROUP BY c.id, c.label, c.slug
        ORDER BY event_count DESC
        LIMIT 10
    """)
    extra_analysis['top_categories_by_events'] = [dict(row) for row in cursor.fetchall()]
    
    # Parent categories
    cursor.execute("""
        SELECT COUNT(*) as parent_categories
        FROM categories
        WHERE parent_category IS NOT NULL
    """)
    extra_analysis['parent_categories'] = cursor.fetchone()['parent_categories']
    
    analysis['category_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_collections_detailed(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Detailed analysis of collections table"""
    analysis = analyze_table_comprehensive(conn, 'collections')
    
    cursor = conn.cursor()
    
    # Additional collection-specific analysis
    extra_analysis = {}
    
    # Collection status
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed_count,
            SUM(CASE WHEN archived = 1 THEN 1 ELSE 0 END) as archived_count,
            SUM(CASE WHEN featured = 1 THEN 1 ELSE 0 END) as featured_count
        FROM collections
    """)
    status_row = cursor.fetchone()
    extra_analysis['status_breakdown'] = dict(status_row) if status_row else {}
    
    # Collections with events
    cursor.execute("""
        SELECT collection_id, COUNT(*) as event_count
        FROM event_collections
        GROUP BY collection_id
        ORDER BY event_count DESC
        LIMIT 10
    """)
    extra_analysis['top_collections_by_events'] = [dict(row) for row in cursor.fetchall()]
    
    analysis['collection_specific_analysis'] = extra_analysis
    
    return analysis

def analyze_relationship_tables(conn: sqlite3.Connection) -> Dict[str, Any]:
    """Analyze all relationship tables"""
    cursor = conn.cursor()
    
    relationship_analysis = {}
    
    # Event relationships
    event_relations = [
        'event_tags', 'event_categories', 'event_series', 'event_collections',
        'event_event_creators', 'event_chats', 'event_templates'
    ]
    
    for table in event_relations:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            relationship_analysis[table] = {
                'row_count': count,
                'sample_data': get_sample_rows(conn, table, 5)
            }
        except:
            relationship_analysis[table] = {'row_count': 0, 'error': 'Table not found'}
    
    # Market relationships
    market_relations = ['market_tags', 'market_categories', 'market_open_interest', 'market_holders']
    
    for table in market_relations:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            relationship_analysis[table] = {
                'row_count': count,
                'sample_data': get_sample_rows(conn, table, 5)
            }
        except:
            relationship_analysis[table] = {'row_count': 0, 'error': 'Table not found'}
    
    # Series relationships
    series_relations = ['series_events', 'series_tags', 'series_categories', 'series_collections', 'series_chats']
    
    for table in series_relations:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            relationship_analysis[table] = {
                'row_count': count,
                'sample_data': get_sample_rows(conn, table, 5)
            }
        except:
            relationship_analysis[table] = {'row_count': 0, 'error': 'Table not found'}
    
    return relationship_analysis

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
        
        # Use detailed analysis for specific tables
        if table == 'events':
            analysis[table] = analyze_events_detailed(conn)
        elif table == 'markets':
            analysis[table] = analyze_markets_detailed(conn)
        elif table == 'series':
            analysis[table] = analyze_series_detailed(conn)
        elif table == 'tags':
            analysis[table] = analyze_tags_detailed(conn)
        elif table == 'categories':
            analysis[table] = analyze_categories_detailed(conn)
        elif table == 'collections':
            analysis[table] = analyze_collections_detailed(conn)
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
    
    # Add relationship tables analysis
    analysis['relationship_tables'] = analyze_relationship_tables(conn)
    
    return analysis

def generate_summary(tables_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate overall summary statistics"""
    summary = {
        'generated_at': datetime.now().isoformat(),
        'database_health': 'healthy',
        'tables_overview': {},
        'data_quality_metrics': {},
        'core_entities_summary': {}
    }
    
    # Tables overview
    for table_name, table_data in tables_analysis.items():
        if table_name != 'relationship_tables':
            summary['tables_overview'][table_name] = {
                'row_count': table_data.get('row_count', 0),
                'column_count': len(table_data.get('columns', [])),
                'has_data': table_data.get('row_count', 0) > 0
            }
    
    # Core entities summary
    if 'events' in tables_analysis:
        events_analysis = tables_analysis['events'].get('event_specific_analysis', {})
        summary['core_entities_summary']['events'] = {
            'total': tables_analysis['events'].get('row_count', 0),
            'active': events_analysis.get('status_breakdown', {}).get('active_count', 0),
            'closed': events_analysis.get('status_breakdown', {}).get('closed_count', 0),
            'with_markets': events_analysis.get('events_with_markets', 0),
            'with_tags': events_analysis.get('events_with_tags', 0)
        }
    
    if 'markets' in tables_analysis:
        markets_analysis = tables_analysis['markets'].get('market_specific_analysis', {})
        summary['core_entities_summary']['markets'] = {
            'total': tables_analysis['markets'].get('row_count', 0),
            'active': markets_analysis.get('status_breakdown', {}).get('active_count', 0),
            'closed': markets_analysis.get('status_breakdown', {}).get('closed_count', 0),
            'funded': markets_analysis.get('status_breakdown', {}).get('funded_count', 0),
            'with_tags': markets_analysis.get('markets_with_tags', 0)
        }
    
    if 'series' in tables_analysis:
        series_analysis = tables_analysis['series'].get('series_specific_analysis', {})
        summary['core_entities_summary']['series'] = {
            'total': tables_analysis['series'].get('row_count', 0),
            'active': series_analysis.get('status_breakdown', {}).get('active_count', 0),
            'with_tags': series_analysis.get('series_with_tags', 0),
            'with_categories': series_analysis.get('series_with_categories', 0)
        }
    
    if 'tags' in tables_analysis:
        tags_analysis = tables_analysis['tags'].get('tag_specific_analysis', {})
        summary['core_entities_summary']['tags'] = {
            'total': tables_analysis['tags'].get('row_count', 0),
            'on_events': tags_analysis.get('usage_stats', {}).get('tags_on_events', 0),
            'on_markets': tags_analysis.get('usage_stats', {}).get('tags_on_markets', 0),
            'on_series': tags_analysis.get('usage_stats', {}).get('tags_on_series', 0),
            'carousel_tags': tags_analysis.get('carousel_tags', 0)
        }
    
    # Calculate data quality metrics
    total_tables = len([t for t in tables_analysis.keys() if t != 'relationship_tables'])
    tables_with_data = sum(1 for t in tables_analysis.values() 
                          if t != 'relationship_tables' and t.get('row_count', 0) > 0)
    
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
    
    # Check markets table
    if 'markets' in tables_analysis:
        markets_data = tables_analysis['markets']
        if markets_data.get('row_count', 0) == 0:
            issues.append("Markets table is empty")
        elif 'market_specific_analysis' in markets_data:
            status = markets_data['market_specific_analysis'].get('status_breakdown', {})
            if status.get('closed_count', 0) > status.get('active_count', 0):
                issues.append("More closed markets than active markets")
    
    # Check other critical tables
    critical_tables = ['tags', 'series', 'users']
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
    
    # Core entities summary
    if 'core_entities_summary' in summary:
        print("\n🎯 CORE ENTITIES:")
        
        # Events
        if 'events' in summary['core_entities_summary']:
            events = summary['core_entities_summary']['events']
            print(f"\n  📅 Events:")
            print(f"     Total:         {events['total']:>10,}")
            print(f"     Active:        {events['active']:>10,}")
            print(f"     Closed:        {events['closed']:>10,}")
            print(f"     With Markets:  {events['with_markets']:>10,}")
            print(f"     With Tags:     {events['with_tags']:>10,}")
        
        # Markets
        if 'markets' in summary['core_entities_summary']:
            markets = summary['core_entities_summary']['markets']
            print(f"\n  📈 Markets:")
            print(f"     Total:         {markets['total']:>10,}")
            print(f"     Active:        {markets['active']:>10,}")
            print(f"     Closed:        {markets['closed']:>10,}")
            print(f"     Funded:        {markets['funded']:>10,}")
            print(f"     With Tags:     {markets['with_tags']:>10,}")
        
        # Series
        if 'series' in summary['core_entities_summary']:
            series = summary['core_entities_summary']['series']
            print(f"\n  📚 Series:")
            print(f"     Total:         {series['total']:>10,}")
            print(f"     Active:        {series['active']:>10,}")
            print(f"     With Tags:     {series['with_tags']:>10,}")
            print(f"     With Categories: {series['with_categories']:>10,}")
        
        # Tags
        if 'tags' in summary['core_entities_summary']:
            tags = summary['core_entities_summary']['tags']
            print(f"\n  🏷️ Tags:")
            print(f"     Total:         {tags['total']:>10,}")
            print(f"     On Events:     {tags['on_events']:>10,}")
            print(f"     On Markets:    {tags['on_markets']:>10,}")
            print(f"     On Series:     {tags['on_series']:>10,}")
            print(f"     Carousel:      {tags['carousel_tags']:>10,}")
    
    print("\n📋 TABLES OVERVIEW:")
    tables_to_show = [
        'events', 'markets', 'series', 'tags', 'categories', 'collections',
        'event_tags', 'market_tags', 'series_tags', 'users', 'comments', 
        'transactions', 'user_positions_current'
    ]
    
    for table in tables_to_show:
        if table in summary.get('tables_overview', {}):
            info = summary['tables_overview'][table]
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
    
    # Show volume statistics if available
    for table_name in ['events', 'markets', 'series']:
        if table_name in analysis and f'{table_name[:-1]}_specific_analysis' in analysis[table_name]:
            specific = analysis[table_name][f'{table_name[:-1]}_specific_analysis']
            volume = specific.get('volume_stats', {})
            
            if volume.get('total_volume'):
                print(f"\n💰 {table_name.upper()} VOLUME:")
                print(f"  Total Volume:        ${volume.get('total_volume', 0):>15,.2f}")
                print(f"  Average Volume:      ${volume.get('avg_volume', 0):>15,.2f}")
                print(f"  Max Volume:          ${volume.get('max_volume', 0):>15,.2f}")

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
    print("📊 POLYMARKET DATABASE ANALYZER - COMPREHENSIVE")
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