#!/usr/bin/env python3
"""
Polymarket Data Analyzer - Multi-File Output Version
Generates separate analysis files for each table and a summary report
"""

import sys
import json
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import os

def connect_db(db_path: str) -> sqlite3.Connection:
    """Connect to the database"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_output_dir(base_path: str = "analyze_data") -> Path:
    """Ensure output directory exists"""
    output_dir = Path(base_path)
    output_dir.mkdir(exist_ok=True)
    return output_dir

def format_value(value: Any, max_length: int = 50) -> str:
    """Format a value for compact display"""
    if value is None:
        return "NULL"
    elif isinstance(value, (dict, list)):
        json_str = json.dumps(value, separators=(',', ':'))
        if len(json_str) > max_length:
            return json_str[:max_length] + "..."
        return json_str
    elif isinstance(value, str):
        if len(value) > max_length:
            return value[:max_length] + "..."
        return value
    else:
        return str(value)

def get_table_info(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    """Get detailed information about a table including columns and data types"""
    cursor = conn.cursor()
    
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

def analyze_numeric_column(conn: sqlite3.Connection, table_name: str, col_name: str) -> Dict[str, Any]:
    """Analyze a numeric column"""
    cursor = conn.cursor()
    
    analysis = {}
    
    try:
        # Basic statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT({col_name}) as non_null,
                MIN({col_name}) as min_val,
                MAX({col_name}) as max_val,
                AVG({col_name}) as avg_val,
                SUM({col_name}) as sum_val
            FROM {table_name}
        """)
        stats = cursor.fetchone()
        
        analysis['total_rows'] = stats['total']
        analysis['non_null'] = stats['non_null']
        analysis['null_count'] = stats['total'] - stats['non_null']
        analysis['null_percentage'] = round((analysis['null_count'] / max(stats['total'], 1)) * 100, 2)
        
        if stats['non_null'] > 0:
            analysis['min'] = stats['min_val']
            analysis['max'] = stats['max_val']
            analysis['avg'] = round(stats['avg_val'], 2) if stats['avg_val'] else 0
            analysis['sum'] = stats['sum_val']
            
            # Get median
            cursor.execute(f"""
                SELECT {col_name} FROM {table_name}
                WHERE {col_name} IS NOT NULL
                ORDER BY {col_name}
                LIMIT 1 OFFSET (SELECT COUNT(*)/2 FROM {table_name} WHERE {col_name} IS NOT NULL)
            """)
            median_row = cursor.fetchone()
            if median_row:
                analysis['median'] = median_row[0]
            
            # Count distinct values
            cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) as distinct_count FROM {table_name}")
            analysis['distinct_values'] = cursor.fetchone()['distinct_count']
            
            # Get value distribution for small cardinality
            if analysis['distinct_values'] <= 20:
                cursor.execute(f"""
                    SELECT {col_name} as value, COUNT(*) as count
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                    GROUP BY {col_name}
                    ORDER BY count DESC
                    LIMIT 10
                """)
                analysis['value_distribution'] = [
                    {'value': row['value'], 'count': row['count']} 
                    for row in cursor.fetchall()
                ]
        
    except Exception as e:
        analysis['error'] = str(e)
    
    return analysis

def analyze_text_column(conn: sqlite3.Connection, table_name: str, col_name: str) -> Dict[str, Any]:
    """Analyze a text column"""
    cursor = conn.cursor()
    
    analysis = {}
    
    try:
        # Basic counts
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT({col_name}) as non_null,
                COUNT(DISTINCT {col_name}) as unique_count
            FROM {table_name}
        """)
        stats = cursor.fetchone()
        
        analysis['total_rows'] = stats['total']
        analysis['non_null'] = stats['non_null']
        analysis['null_count'] = stats['total'] - stats['non_null']
        analysis['null_percentage'] = round((analysis['null_count'] / max(stats['total'], 1)) * 100, 2)
        analysis['unique_values'] = stats['unique_count']
        analysis['uniqueness_ratio'] = round((stats['unique_count'] / max(stats['non_null'], 1)) * 100, 2)
        
        if stats['non_null'] > 0:
            # String length statistics
            cursor.execute(f"""
                SELECT 
                    MIN(LENGTH({col_name})) as min_length,
                    MAX(LENGTH({col_name})) as max_length,
                    AVG(LENGTH({col_name})) as avg_length
                FROM {table_name}
                WHERE {col_name} IS NOT NULL
            """)
            length_stats = cursor.fetchone()
            
            analysis['min_length'] = length_stats['min_length']
            analysis['max_length'] = length_stats['max_length']
            analysis['avg_length'] = round(length_stats['avg_length'], 2) if length_stats['avg_length'] else 0
            
            # Sample values
            cursor.execute(f"""
                SELECT DISTINCT {col_name} 
                FROM {table_name} 
                WHERE {col_name} IS NOT NULL 
                LIMIT 5
            """)
            analysis['sample_values'] = [format_value(row[0], 100) for row in cursor.fetchall()]
            
            # Most common values if not too unique
            if analysis['uniqueness_ratio'] < 50:
                cursor.execute(f"""
                    SELECT {col_name} as value, COUNT(*) as count
                    FROM {table_name}
                    WHERE {col_name} IS NOT NULL
                    GROUP BY {col_name}
                    ORDER BY count DESC
                    LIMIT 5
                """)
                analysis['top_values'] = [
                    {'value': format_value(row['value'], 50), 'count': row['count']} 
                    for row in cursor.fetchall()
                ]
        
    except Exception as e:
        analysis['error'] = str(e)
    
    return analysis

def analyze_datetime_column(conn: sqlite3.Connection, table_name: str, col_name: str) -> Dict[str, Any]:
    """Analyze a datetime column"""
    cursor = conn.cursor()
    
    analysis = {}
    
    try:
        # Basic statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT({col_name}) as non_null,
                MIN({col_name}) as min_date,
                MAX({col_name}) as max_date
            FROM {table_name}
        """)
        stats = cursor.fetchone()
        
        analysis['total_rows'] = stats['total']
        analysis['non_null'] = stats['non_null']
        analysis['null_count'] = stats['total'] - stats['non_null']
        analysis['null_percentage'] = round((analysis['null_count'] / max(stats['total'], 1)) * 100, 2)
        
        if stats['non_null'] > 0:
            analysis['earliest'] = stats['min_date']
            analysis['latest'] = stats['max_date']
            
            # Count distinct dates
            cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) as distinct_count FROM {table_name}")
            analysis['distinct_values'] = cursor.fetchone()['distinct_count']
            
    except Exception as e:
        analysis['error'] = str(e)
    
    return analysis

def analyze_table_columns(conn: sqlite3.Connection, table_name: str) -> Dict[str, Dict[str, Any]]:
    """Analyze all columns in a table"""
    columns_info = get_table_info(conn, table_name)
    column_analysis = {}
    
    for col in columns_info:
        col_name = col['name']
        col_type = col['type'].upper()
        
        analysis = {
            'type': col['type'],
            'nullable': col['nullable'],
            'primary_key': col['primary_key'],
            'default': col['default']
        }
        
        # Analyze based on data type
        if 'INT' in col_type or 'REAL' in col_type or 'NUMERIC' in col_type:
            analysis.update(analyze_numeric_column(conn, table_name, col_name))
        elif 'TEXT' in col_type or 'CHAR' in col_type:
            # Check if it might be a datetime
            if any(dt in col_name.lower() for dt in ['date', 'time', 'created', 'updated', 'at']):
                analysis.update(analyze_datetime_column(conn, table_name, col_name))
            else:
                analysis.update(analyze_text_column(conn, table_name, col_name))
        else:
            # Basic analysis for other types
            analysis.update(analyze_text_column(conn, table_name, col_name))
        
        column_analysis[col_name] = analysis
    
    return column_analysis

def get_sample_rows(conn: sqlite3.Connection, table_name: str, limit: int = 5) -> List[str]:
    """Get sample rows from a table formatted as compact strings"""
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        rows = []
        
        for row in cursor.fetchall():
            # Convert row to dict and format as compact string
            row_dict = dict(row)
            formatted_items = []
            for key, value in row_dict.items():
                formatted_value = format_value(value, 30)
                formatted_items.append(f"{key}:{formatted_value}")
            
            # Join items with | separator for compact display
            row_str = " | ".join(formatted_items[:10])  # Limit to first 10 fields
            if len(row_dict) > 10:
                row_str += " | ..."
            rows.append(row_str)
        
        return rows
    except Exception as e:
        return [f"Error getting sample: {e}"]

def analyze_table_relationships(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Analyze foreign key relationships for a table"""
    cursor = conn.cursor()
    
    relationships = {
        'foreign_keys': [],
        'referenced_by': []
    }
    
    try:
        # Get foreign keys from this table
        cursor.execute(f"PRAGMA foreign_key_list({table_name})")
        for row in cursor.fetchall():
            relationships['foreign_keys'].append({
                'column': row[3],
                'references_table': row[2],
                'references_column': row[4]
            })
        
        # Find tables that reference this table
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name != ?
        """, (table_name,))
        
        for other_table in cursor.fetchall():
            cursor.execute(f"PRAGMA foreign_key_list({other_table[0]})")
            for fk in cursor.fetchall():
                if fk[2] == table_name:  # If references our table
                    relationships['referenced_by'].append({
                        'table': other_table[0],
                        'column': fk[3]
                    })
        
    except Exception as e:
        relationships['error'] = str(e)
    
    return relationships

def analyze_table_comprehensive(conn: sqlite3.Connection, table_name: str) -> Dict[str, Any]:
    """Comprehensive analysis of a single table"""
    cursor = conn.cursor()
    
    print(f"  Analyzing {table_name}...")
    
    analysis = {
        'table_name': table_name,
        'row_count': 0,
        'columns': get_table_info(conn, table_name),
        'column_analysis': {},
        'sample_rows': [],
        'indexes': [],
        'relationships': {}
    }
    
    # Get row count
    try:
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        analysis['row_count'] = cursor.fetchone()['count']
    except:
        analysis['row_count'] = 0
    
    # Analyze columns if table has data
    if analysis['row_count'] > 0:
        analysis['column_analysis'] = analyze_table_columns(conn, table_name)
        analysis['sample_rows'] = get_sample_rows(conn, table_name, 5)
    
    # Get indexes
    cursor.execute(f"PRAGMA index_list({table_name})")
    for row in cursor.fetchall():
        analysis['indexes'].append({
            'name': row[1],
            'unique': row[2] == 1
        })
    
    # Get relationships
    analysis['relationships'] = analyze_table_relationships(conn, table_name)
    
    return analysis

def generate_table_summary(analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Generate a summary of table analysis for the main report"""
    summary = {
        'table_name': analysis['table_name'],
        'row_count': analysis['row_count'],
        'column_count': len(analysis['columns']),
        'index_count': len(analysis['indexes']),
        'foreign_keys': len(analysis['relationships'].get('foreign_keys', [])),
        'referenced_by': len(analysis['relationships'].get('referenced_by', [])),
        'has_data': analysis['row_count'] > 0
    }
    
    # Add key statistics if table has data
    if analysis['row_count'] > 0 and analysis.get('column_analysis'):
        # Find primary key columns
        pk_columns = [col['name'] for col in analysis['columns'] if col.get('primary_key')]
        if pk_columns:
            summary['primary_keys'] = pk_columns
        
        # Calculate data quality metrics
        total_nulls = 0
        total_cells = 0
        
        for col_name, col_data in analysis['column_analysis'].items():
            if 'null_count' in col_data:
                total_nulls += col_data['null_count']
                total_cells += col_data.get('total_rows', 0)
        
        if total_cells > 0:
            summary['null_percentage'] = round((total_nulls / total_cells) * 100, 2)
    
    return summary

def save_table_analysis(output_dir: Path, table_name: str, analysis: Dict[str, Any]):
    """Save detailed table analysis to a separate file"""
    file_path = output_dir / f"table_{table_name}.json"
    
    with open(file_path, 'w') as f:
        json.dump(analysis, f, indent=2, default=str)
    
    print(f"    Saved to {file_path}")

def analyze_all_tables(conn: sqlite3.Connection, output_dir: Path) -> Dict[str, Any]:
    """Analyze all tables and save to separate files"""
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' 
        ORDER BY name
    """)
    tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n🔍 Analyzing {len(tables)} tables...")
    
    summaries = {}
    
    for table in tables:
        # Analyze table
        analysis = analyze_table_comprehensive(conn, table)
        
        # Save detailed analysis to separate file
        save_table_analysis(output_dir, table, analysis)
        
        # Generate and store summary
        summaries[table] = generate_table_summary(analysis)
    
    return summaries

def generate_database_overview(summaries: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Generate overall database overview"""
    overview = {
        'total_tables': len(summaries),
        'total_rows': sum(s['row_count'] for s in summaries.values()),
        'tables_with_data': sum(1 for s in summaries.values() if s['has_data']),
        'empty_tables': sum(1 for s in summaries.values() if not s['has_data']),
        'total_columns': sum(s['column_count'] for s in summaries.values()),
        'total_indexes': sum(s['index_count'] for s in summaries.values()),
        'total_foreign_keys': sum(s['foreign_keys'] for s in summaries.values())
    }
    
    # Find largest tables
    tables_by_size = sorted(
        [(name, info['row_count']) for name, info in summaries.items()],
        key=lambda x: x[1],
        reverse=True
    )
    overview['largest_tables'] = tables_by_size[:10]
    
    # Group tables by category
    categories = {
        'core': ['events', 'markets', 'series', 'tags', 'categories', 'collections'],
        'user': ['users', 'user_activity', 'user_trades', 'user_positions_current', 
                 'user_positions_closed', 'user_values'],
        'transaction': ['transactions', 'comments', 'comment_reactions'],
        'relationship': [t for t in summaries.keys() if '_' in t and any(
            rel in t for rel in ['event_', 'market_', 'series_', 'tag_']
        )],
        'tracking': ['event_live_volume', 'market_open_interest', 'market_holders']
    }
    
    overview['categories'] = {}
    for category, tables in categories.items():
        category_tables = [t for t in tables if t in summaries]
        overview['categories'][category] = {
            'tables': category_tables,
            'count': len(category_tables),
            'total_rows': sum(summaries[t]['row_count'] for t in category_tables if t in summaries)
        }
    
    return overview

def print_summary(report: Dict[str, Any]):
    """Print a formatted summary to console"""
    overview = report.get('overview', {})
    summaries = report.get('table_summaries', {})
    
    print("\n" + "=" * 80)
    print("📊 DATABASE ANALYSIS COMPLETE")
    print("=" * 80)
    
    print(f"\n📈 DATABASE OVERVIEW:")
    print(f"  Total Tables:        {overview.get('total_tables', 0)}")
    print(f"  Total Rows:          {overview.get('total_rows', 0):,}")
    print(f"  Tables with Data:    {overview.get('tables_with_data', 0)}")
    print(f"  Empty Tables:        {overview.get('empty_tables', 0)}")
    print(f"  Total Columns:       {overview.get('total_columns', 0)}")
    print(f"  Total Indexes:       {overview.get('total_indexes', 0)}")
    print(f"  Total Foreign Keys:  {overview.get('total_foreign_keys', 0)}")
    
    print("\n📊 TABLE CATEGORIES:")
    for category, info in overview.get('categories', {}).items():
        if info['count'] > 0:
            print(f"\n  {category.upper()}:")
            print(f"    Tables: {info['count']}")
            print(f"    Rows:   {info['total_rows']:,}")
    
    print("\n🏆 LARGEST TABLES:")
    for table, count in overview.get('largest_tables', [])[:5]:
        status = "✅" if count > 0 else "⚠️"
        print(f"  {status} {table:<30} {count:>10,} rows")
    
    print("\n📁 OUTPUT FILES:")
    print(f"  Main Report:     analyze_data/report.json")
    print(f"  Table Details:   analyze_data/table_*.json")
    print(f"  Total Files:     {len(summaries) + 1}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Analyze Polymarket Terminal Database')
    parser.add_argument('--db', type=str, default='polymarket_terminal.db',
                       help='Path to database file')
    parser.add_argument('--output-dir', type=str, default='analyze_data',
                       help='Output directory for analysis files')
    
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
    print(f"📂 Output Directory: {args.output_dir}")
    
    # Ensure output directory exists
    output_dir = ensure_output_dir(args.output_dir)
    
    # Connect to database
    conn = connect_db(args.db)
    
    # Analyze all tables (saves individual files)
    table_summaries = analyze_all_tables(conn, output_dir)
    
    # Generate database overview
    overview = generate_database_overview(table_summaries)
    
    # Create main report
    report = {
        'generated_at': datetime.now().isoformat(),
        'database_path': str(args.db),
        'overview': overview,
        'table_summaries': table_summaries
    }
    
    # Save main report
    report_path = output_dir / 'report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    # Close connection
    conn.close()
    
    # Print summary
    print_summary(report)
    
    print("\n✨ Analysis complete!")

if __name__ == "__main__":
    main()