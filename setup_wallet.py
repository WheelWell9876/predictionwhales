#!/usr/bin/env python3
"""
Polygon Wallet Setup for Polymarket Observer
Creates a new wallet or imports existing one for read-only observation
"""

import os
import json
import secrets
from eth_account import Account
from web3 import Web3
from dotenv import load_dotenv, set_key
import getpass


def create_new_wallet():
    """Create a new Polygon wallet for observation"""
    print("\n🔐 Creating new Polygon wallet for observation...")

    # Generate new account
    account = Account.create(secrets.token_hex(32))

    wallet_info = {
        'address': account.address,
        'private_key': account.key.hex(),
        'warning': 'This is an OBSERVER wallet - DO NOT send funds unless needed for gas'
    }

    print(f"\n✅ New wallet created!")
    print(f"📍 Address: {wallet_info['address']}")
    print(f"🔑 Private Key: {wallet_info['private_key']}")
    print(f"\n⚠️  IMPORTANT: Save these credentials securely!")
    print(f"⚠️  This wallet is for OBSERVATION ONLY - no funds needed initially")

    return wallet_info


def import_existing_wallet():
    """Import existing wallet"""
    print("\n🔑 Import existing wallet")
    private_key = getpass.getpass("Enter private key (hidden): ").strip()

    try:
        # Remove 0x prefix if present
        if private_key.startswith('0x'):
            private_key = private_key[2:]

        # Validate and create account
        account = Account.from_key(private_key)

        wallet_info = {
            'address': account.address,
            'private_key': '0x' + private_key,
            'warning': 'Imported wallet - ensure it has no funds if only observing'
        }

        print(f"\n✅ Wallet imported successfully!")
        print(f"📍 Address: {wallet_info['address']}")

        return wallet_info

    except Exception as e:
        print(f"❌ Error importing wallet: {e}")
        return None


def check_polygon_balance(address, rpc_url="https://polygon-rpc.com"):
    """Check MATIC balance on Polygon"""
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url))
        if w3.is_connected():
            balance = w3.eth.get_balance(address)
            matic_balance = w3.from_wei(balance, 'ether')
            print(f"\n💰 Current MATIC balance: {matic_balance} MATIC")

            if matic_balance > 0:
                print("⚠️  Warning: This wallet has funds. Use a different wallet if only observing!")
            else:
                print("✅ Wallet has no funds - safe for observation only")

            return float(matic_balance)
        else:
            print("⚠️  Could not connect to Polygon network")
            return None
    except Exception as e:
        print(f"Error checking balance: {e}")
        return None


def save_to_env(wallet_info):
    """Save wallet info to .env file"""
    env_file = '.env'

    # Create .env if it doesn't exist
    if not os.path.exists(env_file):
        with open(env_file, 'w') as f:
            f.write("# Polymarket Whale Tracker Configuration\n\n")

    # Save wallet info
    set_key(env_file, 'OBSERVER_PRIVATE_KEY', wallet_info['private_key'])
    set_key(env_file, 'OBSERVER_ADDRESS', wallet_info['address'])
    set_key(env_file, 'POLYGON_RPC_URL', 'https://polygon-rpc.com')

    print(f"\n✅ Wallet credentials saved to {env_file}")


def setup_polymarket_config():
    """Setup additional Polymarket configuration"""
    env_file = '.env'

    # API Configuration
    set_key(env_file, 'MIN_BET_AMOUNT', '1000')
    set_key(env_file, 'MIN_WHALE_VOLUME', '10000')
    set_key(env_file, 'UPDATE_INTERVAL', '300')
    set_key(env_file, 'MAX_TRACKED_WALLETS', '100')
    set_key(env_file, 'MAX_CONCURRENT_REQUESTS', '10')

    print("✅ Polymarket configuration saved")


def main():
    """Main setup function"""
    print("=" * 50)
    print("🐋 Polymarket Whale Tracker - Wallet Setup")
    print("=" * 50)

    print("\nThis tool will set up a Polygon wallet for observing")
    print("Polymarket transactions. No funds needed initially.")

    print("\nOptions:")
    print("1. Create new wallet (recommended for observation)")
    print("2. Import existing wallet")
    print("3. Exit")

    choice = input("\nSelect option (1-3): ").strip()

    wallet_info = None

    if choice == '1':
        wallet_info = create_new_wallet()
    elif choice == '2':
        wallet_info = import_existing_wallet()
    elif choice == '3':
        print("Exiting...")
        return
    else:
        print("Invalid option")
        return

    if wallet_info:
        # Check balance
        check_polygon_balance(wallet_info['address'])

        # Save to environment
        save_choice = input("\nSave wallet to .env file? (y/n): ").strip().lower()
        if save_choice == 'y':
            save_to_env(wallet_info)
            setup_polymarket_config()

            print("\n" + "=" * 50)
            print("✅ Setup Complete!")
            print("=" * 50)
            print("\nNext steps:")
            print("1. Your observer wallet is ready")
            print("2. No funds needed for observation")
            print("3. Run 'python main.py' to start the tracker")
            print("4. The system will automatically get API credentials")

            print("\n📝 Notes:")
            print("- This wallet only observes blockchain data")
            print("- No transactions will be made")
            print("- API credentials will be obtained automatically")
            print("- Data is pulled from public Polymarket APIs")

            # Save wallet info to separate file for backup
            with open('wallet_backup.json', 'w') as f:
                json.dump(wallet_info, f, indent=2)
            print(f"\n🔐 Wallet backup saved to wallet_backup.json")
            print("⚠️  Keep this file secure and don't commit to git!")


if __name__ == "__main__":
    main()