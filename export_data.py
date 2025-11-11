"""
Database Export Utility for Polymarket Terminal
Exports sample data from all tables to JSON files for examination
"""

import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class DataExporter:
    """Export database tables to JSON files for examination"""

    def __init__(self, db_path: str = 'polymarket_terminal.db', output_dir: str = 'backend/data/tables_ex'):
        self.db_path = db_path
        self.output_dir = output_dir
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

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            ORDER BY name
        """)
        return [row[0] for row in cursor.fetchall()]

    def get_table_info(self, table_name: str) -> Dict:
        """Get information about a table"""
        cursor = self.conn.cursor()

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]

        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [{'name': row[1], 'type': row[2], 'nullable': not row[3], 'default': row[4], 'pk': row[5]}
                   for row in cursor.fetchall()]

        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': columns
        }

    def export_table_sample(self, table_name: str, limit: int = 100) -> Dict:
        """Export sample data from a table"""
        cursor = self.conn.cursor()

        # Get table info
        info = self.get_table_info(table_name)

        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
        rows = cursor.fetchall()

        # Convert rows to dictionaries
        data = []
        for row in rows:
            row_dict = {}
            for key in row.keys():
                value = row[key]
                # Handle datetime objects and other non-serializable types
                if isinstance(value, (datetime, bytes)):
                    row_dict[key] = str(value)
                else:
                    row_dict[key] = value
            data.append(row_dict)

        return {
            'metadata': {
                'table_name': table_name,
                'total_rows': info['row_count'],
                'sample_size': len(data),
                'columns': info['columns'],
                'exported_at': datetime.now().isoformat()
            },
            'data': data
        }

    def export_all_tables(self, limit: int = 100):
        """Export all tables to JSON files"""
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Connect to database
        self.connect()

        # Get all tables
        tables = self.get_all_tables()

        print(f"📊 Found {len(tables)} tables in database")
        print(f"📁 Exporting to: {self.output_dir}")
        print("=" * 60)

        # Track statistics
        total_rows_exported = 0
        tables_exported = []
        empty_tables = []

        for table in tables:
            try:
                # Export table sample
                export_data = self.export_table_sample(table, limit)

                # Write to JSON file
                output_file = os.path.join(self.output_dir, f"{table}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)

                rows_in_sample = len(export_data['data'])
                total_rows = export_data['metadata']['total_rows']

                if rows_in_sample > 0:
                    print(f"✅ {table}: {rows_in_sample}/{total_rows} rows exported")
                    tables_exported.append(table)
                    total_rows_exported += rows_in_sample
                else:
                    print(f"⚠️  {table}: Empty table (0 rows)")
                    empty_tables.append(table)

            except Exception as e:
                print(f"❌ {table}: Error - {e}")

        # Disconnect
        self.disconnect()

        # Print summary
        print("=" * 60)
        print("📊 Export Summary:")
        print(f"   Total tables processed: {len(tables)}")
        print(f"   Tables with data: {len(tables_exported)}")
        print(f"   Empty tables: {len(empty_tables)}")
        print(f"   Total rows exported: {total_rows_exported}")
        print(f"   Output directory: {self.output_dir}")

        # Create summary file
        summary = {
            'export_timestamp': datetime.now().isoformat(),
            'database_path': self.db_path,
            'total_tables': len(tables),
            'tables_with_data': tables_exported,
            'empty_tables': empty_tables,
            'total_rows_exported': total_rows_exported,
            'export_limit_per_table': limit
        }

        summary_file = os.path.join(self.output_dir, '_summary.json')
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        print(f"\n✅ Export complete! Check {self.output_dir} for JSON files")

        return summary

    def export_specific_tables(self, table_names: List[str], limit: int = 100):
        """Export specific tables only"""
        # Create output directory if it doesn't exist
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        # Connect to database
        self.connect()

        print(f"📊 Exporting {len(table_names)} tables")
        print(f"📁 Output directory: {self.output_dir}")
        print("=" * 60)

        for table in table_names:
            try:
                # Check if table exists
                cursor = self.conn.cursor()
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name=?
                """, (table,))

                if not cursor.fetchone():
                    print(f"⚠️  {table}: Table does not exist")
                    continue

                # Export table sample
                export_data = self.export_table_sample(table, limit)

                # Write to JSON file
                output_file = os.path.join(self.output_dir, f"{table}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)

                rows_in_sample = len(export_data['data'])
                total_rows = export_data['metadata']['total_rows']

                print(f"✅ {table}: {rows_in_sample}/{total_rows} rows exported")

            except Exception as e:
                print(f"❌ {table}: Error - {e}")

        # Disconnect
        self.disconnect()

        print("=" * 60)
        print(f"✅ Export complete! Check {self.output_dir} for JSON files")


def main():
    """Main function to run the export"""
    import argparse

    parser = argparse.ArgumentParser(description='Export Polymarket database tables to JSON')
    parser.add_argument('--db', default='polymarket_terminal.db', help='Database path')
    parser.add_argument('--output', default='backend/data/tables_ex', help='Output directory')
    parser.add_argument('--limit', type=int, default=100, help='Number of rows per table')
    parser.add_argument('--tables', nargs='+', help='Specific tables to export (optional)')

    args = parser.parse_args()

    # Create exporter
    exporter = DataExporter(db_path=args.db, output_dir=args.output)

    # Export tables
    if args.tables:
        exporter.export_specific_tables(args.tables, limit=args.limit)
    else:
        exporter.export_all_tables(limit=args.limit)


if __name__ == "__main__":
    main()