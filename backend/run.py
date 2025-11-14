#!/usr/bin/env python3
"""
Enhanced run script for Polymarket Terminal with selective operations
"""

import sys
import subprocess
import os

def clear_screen():
    """Clear the terminal screen"""
    os.system('cls' if os.name == 'nt' else 'clear')

def print_main_menu():
    """Print the main menu"""
    print("\n" + "=" * 70)
    print("POLYMARKET TERMINAL - BACKEND OPERATIONS")
    print("=" * 70)
    print("\n📋 MAIN MENU:")
    print("\n1. Initial Setup (first time only)")
    print("2. Full Data Operations")
    print("3. Selective Load Operations")
    print("4. Selective Delete Operations")
    print("5. Daily Operations")
    print("6. Start Flask API Server")
    print("7. Database Management")
    print("8. Exit")
    print("\n" + "=" * 70)

def print_full_data_menu():
    """Print the full data operations menu"""
    clear_screen()
    print("\n" + "=" * 70)
    print("FULL DATA OPERATIONS")
    print("=" * 70)
    print("\n1. Fetch All Data (complete initial load)")
    print("2. Reset Database (delete all data)")
    print("3. Reset and Reload All Data")
    print("4. Show Database Statistics")
    print("5. Back to Main Menu")
    print("\n" + "=" * 70)

def print_selective_load_menu():
    """Print the selective load menu"""
    clear_screen()
    print("\n" + "=" * 70)
    print("SELECTIVE LOAD OPERATIONS")
    print("=" * 70)
    print("\n📥 Load Individual Data Types:")
    print("\n1. Load Tags Only")
    print("2. Load Series Only")
    print("3. Load Events Only (active)")
    print("4. Load Events Only (including closed)")
    print("5. Load Markets Only")
    print("6. Load Users Only (whales)")
    print("7. Load Transactions Only")
    print("8. Load Comments Only")
    print("\n📦 Load Multiple:")
    print("9. Load Core Data (Tags + Events + Markets)")
    print("10. Load User Data (Users + Transactions)")
    print("11. Custom Selection")
    print("\n12. Back to Main Menu")
    print("\n" + "=" * 70)

def print_selective_delete_menu():
    """Print the selective delete menu"""
    clear_screen()
    print("\n" + "=" * 70)
    print("SELECTIVE DELETE OPERATIONS")
    print("=" * 70)
    print("\n🗑️  Delete Individual Data Types:")
    print("\n1. Delete Tags Only")
    print("2. Delete Series Only")
    print("3. Delete Events Only")
    print("4. Delete Markets Only")
    print("5. Delete Users Only")
    print("6. Delete Transactions Only")
    print("7. Delete Comments Only")
    print("\n🧹 Cleanup Operations:")
    print("8. Delete Closed Events Only")
    print("9. Delete All User Data (Users + Transactions)")
    print("10. Custom Selection")
    print("\n11. Back to Main Menu")
    print("\n" + "=" * 70)

def print_daily_operations_menu():
    """Print the daily operations menu"""
    clear_screen()
    print("\n" + "=" * 70)
    print("DAILY OPERATIONS")
    print("=" * 70)
    print("\n1. Run Daily Update Now")
    print("2. Start Automatic Scheduler")
    print("3. Update Whale Profiles Only")
    print("4. Check for New Whales")
    print("5. Back to Main Menu")
    print("\n" + "=" * 70)

def print_database_menu():
    """Print the database management menu"""
    clear_screen()
    print("\n" + "=" * 70)
    print("DATABASE MANAGEMENT")
    print("=" * 70)
    print("\n1. Show Database Statistics")
    print("2. Show Whale Statistics")
    print("3. Backup Database")
    print("4. Vacuum Database (optimize)")
    print("5. Back to Main Menu")
    print("\n" + "=" * 70)

def run_command(cmd, confirm_message=None):
    """Run a command and handle errors"""
    if confirm_message:
        response = input(f"\n⚠️  {confirm_message} (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled")
            return False
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: Command failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

def handle_custom_selection(operation_type):
    """Handle custom selection of data types"""
    data_types = ['tags', 'series', 'events', 'markets', 'users', 'transactions', 'comments']
    selected = []
    
    print(f"\n📝 Select data types to {operation_type}:")
    print("Enter the numbers separated by commas (e.g., 1,3,5)\n")
    
    for i, dtype in enumerate(data_types, 1):
        print(f"{i}. {dtype.capitalize()}")
    
    try:
        choices = input("\nYour selection: ").strip().split(',')
        for choice in choices:
            idx = int(choice.strip()) - 1
            if 0 <= idx < len(data_types):
                selected.append(data_types[idx])
    except (ValueError, IndexError):
        print("Invalid selection")
        return None
    
    return selected

def handle_full_data_operations():
    """Handle full data operations submenu"""
    while True:
        print_full_data_menu()
        
        try:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print("\n📄 Fetching all data from Polymarket...")
                print("This may take 10-30 minutes...")
                run_command("python setup.py --initial-load")
                
            elif choice == '2':
                run_command("python setup.py --reset",
                           "WARNING: This will delete ALL data. Continue?")
                
            elif choice == '3':
                if run_command("python setup.py --reset",
                              "WARNING: This will delete ALL data and reload. Continue?"):
                    run_command("python setup.py --initial-load")
                
            elif choice == '4':
                run_command("python setup.py --stats")
                
            elif choice == '5':
                break
                
            else:
                print("\n❌ Invalid choice. Please enter 1-5")
            
            if choice != '5':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            break

def handle_selective_load():
    """Handle selective load operations submenu"""
    while True:
        print_selective_load_menu()
        
        try:
            choice = input("\nEnter your choice (1-12): ").strip()
            
            if choice == '1':
                run_command("python setup.py --load-tags")
                
            elif choice == '2':
                run_command("python setup.py --load-series")
                
            elif choice == '3':
                run_command("python setup.py --load-events")
                
            elif choice == '4':
                run_command("python setup.py --load-events --closed-events")
                
            elif choice == '5':
                run_command("python setup.py --load-markets")
                
            elif choice == '6':
                run_command("python setup.py --load-users")
                
            elif choice == '7':
                limit = input("Enter market limit (default 20): ").strip()
                if limit.isdigit():
                    run_command(f"python setup.py --load-transactions --limit-markets {limit}")
                else:
                    run_command("python setup.py --load-transactions")
                
            elif choice == '8':
                run_command("python setup.py --load-comments")
                
            elif choice == '9':
                print("\n📦 Loading core data (Tags + Events + Markets)...")
                run_command("python setup.py --load-tags --load-events --load-markets")
                
            elif choice == '10':
                print("\n📦 Loading user data (Users + Transactions)...")
                run_command("python setup.py --load-users --load-transactions")
                
            elif choice == '11':
                selected = handle_custom_selection("load")
                if selected:
                    flags = ' '.join([f'--load-{dtype}' for dtype in selected])
                    run_command(f"python setup.py {flags}")
                
            elif choice == '12':
                break
                
            else:
                print("\n❌ Invalid choice. Please enter 1-12")
            
            if choice != '12':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            break

def handle_selective_delete():
    """Handle selective delete operations submenu"""
    while True:
        print_selective_delete_menu()
        
        try:
            choice = input("\nEnter your choice (1-11): ").strip()
            
            if choice == '1':
                run_command("python setup.py --delete-tags",
                           "WARNING: This will delete all tags. Continue?")
                
            elif choice == '2':
                run_command("python setup.py --delete-series",
                           "WARNING: This will delete all series. Continue?")
                
            elif choice == '3':
                run_command("python setup.py --delete-events",
                           "WARNING: This will delete all events. Continue?")
                
            elif choice == '4':
                run_command("python setup.py --delete-markets",
                           "WARNING: This will delete all markets. Continue?")
                
            elif choice == '5':
                run_command("python setup.py --delete-users",
                           "WARNING: This will delete all users. Continue?")
                
            elif choice == '6':
                run_command("python setup.py --delete-transactions",
                           "WARNING: This will delete all transactions. Continue?")
                
            elif choice == '7':
                run_command("python setup.py --delete-comments",
                           "WARNING: This will delete all comments. Continue?")
                
            elif choice == '8':
                print("\n🧹 Removing closed events only...")
                # This would need a specific command in setup.py
                run_command("python -c \"from backend.database_manager import DatabaseManager; db = DatabaseManager(); print(f'Removed {db.remove_closed_events()} closed events')\"")
                
            elif choice == '9':
                run_command("python setup.py --delete-users --delete-transactions",
                           "WARNING: This will delete all user data. Continue?")
                
            elif choice == '10':
                selected = handle_custom_selection("delete")
                if selected:
                    flags = ' '.join([f'--delete-{dtype}' for dtype in selected])
                    run_command(f"python setup.py {flags}",
                               f"WARNING: This will delete {', '.join(selected)}. Continue?")
                
            elif choice == '11':
                break
                
            else:
                print("\n❌ Invalid choice. Please enter 1-11")
            
            if choice != '11':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            break

def handle_daily_operations():
    """Handle daily operations submenu"""
    while True:
        print_daily_operations_menu()
        
        try:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                print("\n📅 Running daily update...")
                run_command("python setup.py --daily-update")
                
            elif choice == '2':
                print("\n⏰ Starting automatic scheduler...")
                print("Press Ctrl+C to stop and return to menu")
                run_command("python setup.py --scheduler")
                
            elif choice == '3':
                print("\n🐋 Updating whale profiles...")
                # This would need implementation in the data_fetcher
                run_command("python -c \"from backend.data_fetcher import PolymarketDataFetcher; f = PolymarketDataFetcher(); f.load_users_only()\"")
                
            elif choice == '4':
                print("\n🔍 Checking for new whales...")
                # This would need implementation
                run_command("python -c \"from backend.data_fetcher import PolymarketDataFetcher; f = PolymarketDataFetcher(); result = f.users_manager.fetch_top_holders_for_all_markets(); print(f'Found {result.get('total_whales_found', 0)} whales')\"")
                
            elif choice == '5':
                break
                
            else:
                print("\n❌ Invalid choice. Please enter 1-5")
            
            if choice != '5':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            break

def handle_database_management():
    """Handle database management submenu"""
    while True:
        print_database_menu()
        
        try:
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                run_command("python setup.py --stats")
                
            elif choice == '2':
                print("\n🐋 Whale Statistics:")
                run_command("python -c \"from backend.database_manager import DatabaseManager; db = DatabaseManager(); whale_count = db.fetch_one('SELECT COUNT(*) as count FROM users WHERE is_whale = 1'); avg_value = db.fetch_one('SELECT AVG(total_value) as avg FROM users WHERE is_whale = 1 AND total_value > 0'); print(f'Total Whales: {whale_count['count'] if whale_count else 0:,}'); print(f'Average Whale Value: ${avg_value['avg']:,.2f}' if avg_value and avg_value['avg'] else 'N/A')\"")
                
            elif choice == '3':
                print("\n💾 Creating database backup...")
                run_command("python -c \"from backend.database_manager import DatabaseManager; db = DatabaseManager(); backup_path = db.backup_database(); print(f'Backup created: {backup_path}')\"")
                
            elif choice == '4':
                print("\n🔧 Optimizing database...")
                run_command("python -c \"from backend.database_manager import DatabaseManager; import sqlite3; db = DatabaseManager(); conn = db.get_persistent_connection(); conn.execute('VACUUM'); print('Database optimized')\"")
                
            elif choice == '5':
                break
                
            else:
                print("\n❌ Invalid choice. Please enter 1-5")
            
            if choice != '5':
                input("\nPress Enter to continue...")
                
        except KeyboardInterrupt:
            break

def main():
    """Main menu loop with submenus"""
    
    while True:
        clear_screen()
        print_main_menu()
        
        try:
            choice = input("\nEnter your choice (1-8): ").strip()
            
            if choice == '1':
                print("\n🚀 Running initial setup...")
                run_command("python setup.py --setup")
                input("\nPress Enter to continue...")
                
            elif choice == '2':
                handle_full_data_operations()
                
            elif choice == '3':
                handle_selective_load()
                
            elif choice == '4':
                handle_selective_delete()
                
            elif choice == '5':
                handle_daily_operations()
                
            elif choice == '6':
                print("\n🌐 Starting Flask API server...")
                print("Server will run on http://localhost:5000")
                print("Press Ctrl+C to stop and return to menu")
                run_command("python app.py")
                
            elif choice == '7':
                handle_database_management()
                
            elif choice == '8':
                print("\n👋 Goodbye!")
                sys.exit(0)
                
            else:
                print("\n❌ Invalid choice. Please enter 1-8")
                input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\n⚠️ Interrupted. Returning to menu...")
            continue
        except EOFError:
            print("\n\n👋 Goodbye!")
            sys.exit(0)

if __name__ == "__main__":
    print("\n🚀 POLYMARKET TERMINAL BACKEND - ENHANCED")
    print("=" * 45)
    print("Now with selective loading and deletion!")
    print("=" * 45)
    main()