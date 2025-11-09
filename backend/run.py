#!/usr/bin/env python3
"""
Quick run script for common Polymarket Terminal operations
"""

import sys
import subprocess

def print_menu():
    """Print the main menu"""
    print("\n" + "=" * 60)
    print("POLYMARKET TERMINAL - BACKEND OPERATIONS")
    print("=" * 60)
    print("\n1. Initial Setup (first time only)")
    print("2. Fetch All Data (initial load)")
    print("3. Run Daily Update")
    print("4. Start Automatic Scheduler")
    print("5. Start Flask API Server")
    print("6. Show Database Statistics")
    print("7. Reset Database")
    print("8. Exit")
    print("\n" + "=" * 60)

def run_command(cmd):
    """Run a command and handle errors"""
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error: Command failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False

def main():
    """Main menu loop"""
    
    while True:
        print_menu()
        
        try:
            choice = input("\nEnter your choice (1-8): ").strip()
            
            if choice == '1':
                print("\n🚀 Running initial setup...")
                run_command("python setup.py --setup")
                
            elif choice == '2':
                print("\n🔄 Fetching all data from Polymarket...")
                print("This may take 10-30 minutes...")
                run_command("python setup.py --initial-load")
                
            elif choice == '3':
                print("\n📅 Running daily update...")
                run_command("python setup.py --daily-update")
                
            elif choice == '4':
                print("\n⏰ Starting automatic scheduler...")
                print("Press Ctrl+C to stop and return to menu")
                run_command("python setup.py --scheduler")
                
            elif choice == '5':
                print("\n🌐 Starting Flask API server...")
                print("Server will run on http://localhost:5000")
                print("Press Ctrl+C to stop and return to menu")
                run_command("python app.py")
                
            elif choice == '6':
                print("\n📊 Database Statistics:")
                run_command("python setup.py --stats")
                
            elif choice == '7':
                confirm = input("\n⚠️  WARNING: This will delete all data. Continue? (yes/no): ")
                if confirm.lower() == 'yes':
                    run_command("python setup.py --reset")
                else:
                    print("Operation cancelled")
                
            elif choice == '8':
                print("\n👋 Goodbye!")
                sys.exit(0)
                
            else:
                print("\n❌ Invalid choice. Please enter 1-8")
            
            input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\nReturning to menu...")
            continue
        except EOFError:
            print("\n\n👋 Goodbye!")
            sys.exit(0)

if __name__ == "__main__":
    print("\n🚀 POLYMARKET TERMINAL BACKEND")
    print("================================")
    main()