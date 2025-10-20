#!/usr/bin/env python3
"""
Polymarket Whale Tracker - Error Fix Script
Diagnoses and fixes common errors
"""

import os
import sys
import subprocess
import traceback
from pathlib import Path


class ErrorFixer:
    def __init__(self):
        self.errors_found = []
        self.fixes_applied = []

    def check_python_version(self):
        """Check Python version"""
        print("🔍 Checking Python version...")
        version = sys.version_info
        if version.major == 3 and version.minor >= 8:
            print(f"✅ Python {version.major}.{version.minor}.{version.micro} - OK")
            return True
        else:
            print(f"❌ Python {version.major}.{version.minor} - Needs 3.8+")
            self.errors_found.append("Python version < 3.8")
            return False

    def check_directory_structure(self):
        """Check and create directory structure"""
        print("\n🔍 Checking directory structure...")

        required_dirs = [
            'services',
            'models',
            'static',
            'static/css',
            'static/js'
        ]

        for dir_path in required_dirs:
            if not os.path.exists(dir_path):
                print(f"❌ Missing: {dir_path}")
                os.makedirs(dir_path, exist_ok=True)
                print(f"✅ Created: {dir_path}")
                self.fixes_applied.append(f"Created {dir_path}")
            else:
                print(f"✅ Found: {dir_path}")

        # Create __init__.py files
        for module_dir in ['services', 'models']:
            init_file = os.path.join(module_dir, '__init__.py')
            if not os.path.exists(init_file):
                Path(init_file).touch()
                print(f"✅ Created: {init_file}")
                self.fixes_applied.append(f"Created {init_file}")

    def check_dependencies(self):
        """Check and install missing dependencies"""
        print("\n🔍 Checking dependencies...")

        required_packages = {
            'fastapi': 'fastapi',
            'uvicorn': 'uvicorn[standard]',
            'aiohttp': 'aiohttp',
            'web3': 'web3',
            'eth_account': 'eth-account',
            'dotenv': 'python-dotenv',
            'pydantic': 'pydantic',
            'pydantic_settings': 'pydantic-settings'
        }

        missing_packages = []

        for module_name, package_name in required_packages.items():
            try:
                __import__(module_name)
                print(f"✅ {module_name} - installed")
            except ImportError:
                print(f"❌ {module_name} - missing")
                missing_packages.append(package_name)
                self.errors_found.append(f"Missing package: {module_name}")

        if missing_packages:
            print(f"\n📦 Installing missing packages: {', '.join(missing_packages)}")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + missing_packages)
                self.fixes_applied.append(f"Installed: {', '.join(missing_packages)}")
                print("✅ Packages installed successfully")
            except:
                print("❌ Failed to install packages. Run manually:")
                print(f"   pip install {' '.join(missing_packages)}")

    def fix_config_imports(self):
        """Fix common config import issues"""
        print("\n🔍 Checking config.py...")

        if not os.path.exists('config.py'):
            print("❌ config.py missing - creating...")
            config_content = '''from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings"""

    # API Configuration
    POLYMARKET_API_KEY: Optional[str] = None

    # Tracking Configuration
    MIN_BET_AMOUNT: float = 1000
    MIN_WHALE_VOLUME: float = 10000
    UPDATE_INTERVAL: int = 300

    # Database Configuration
    DATABASE_URL: Optional[str] = "sqlite:///./whale_tracker.db"

    # Rate Limiting
    MAX_CONCURRENT_REQUESTS: int = 10
    REQUEST_TIMEOUT: int = 30

    # Tracking Limits
    MAX_TRACKED_WALLETS: int = 100
    MAX_RECENT_BETS: int = 500

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()'''

            with open('config.py', 'w') as f:
                f.write(config_content)

            print("✅ config.py created")
            self.fixes_applied.append("Created config.py")
        else:
            # Check if it uses correct import
            with open('config.py', 'r') as f:
                content = f.read()

            if 'from pydantic import BaseSettings' in content:
                print("⚠️  config.py uses old pydantic import - fixing...")
                content = content.replace(
                    'from pydantic import BaseSettings',
                    'from pydantic_settings import BaseSettings'
                )
                with open('config.py', 'w') as f:
                    f.write(content)
                print("✅ Fixed pydantic import in config.py")
                self.fixes_applied.append("Fixed pydantic import")
            else:
                print("✅ config.py looks good")

    def create_env_file(self):
        """Create .env file if missing"""
        print("\n🔍 Checking .env file...")

        if not os.path.exists('.env'):
            print("❌ .env missing - creating...")
            env_content = '''# Polymarket Whale Tracker Configuration
OBSERVER_PRIVATE_KEY=0x0000000000000000000000000000000000000000000000000000000000000001
OBSERVER_ADDRESS=0x0000000000000000000000000000000000000000
POLYGON_RPC_URL=https://polygon-rpc.com

MIN_BET_AMOUNT=1000
MIN_WHALE_VOLUME=10000
UPDATE_INTERVAL=300
MAX_TRACKED_WALLETS=100
MAX_CONCURRENT_REQUESTS=10'''

            with open('.env', 'w') as f:
                f.write(env_content)

            print("✅ .env file created with defaults")
            self.fixes_applied.append("Created .env file")
        else:
            print("✅ .env file exists")

    def test_imports(self):
        """Test if all imports work"""
        print("\n🔍 Testing imports...")

        test_imports = [
            ('config', 'settings'),
            ('fastapi', 'FastAPI'),
            ('pydantic_settings', 'BaseSettings'),
            ('web3', 'Web3'),
            ('aiohttp', 'ClientSession')
        ]

        for module_name, attr_name in test_imports:
            try:
                module = __import__(module_name)
                if hasattr(module, attr_name):
                    print(f"✅ {module_name}.{attr_name} - OK")
                else:
                    print(f"⚠️  {module_name} imported but {attr_name} not found")
            except ImportError as e:
                print(f"❌ Failed to import {module_name}: {e}")
                self.errors_found.append(f"Import error: {module_name}")
            except Exception as e:
                print(f"❌ Error with {module_name}: {e}")

    def test_api_connectivity(self):
        """Test API connectivity"""
        print("\n🔍 Testing API connectivity...")

        import urllib.request
        import json

        apis = {
            'CLOB': 'https://clob.polymarket.com/trades?limit=1',
            'Gamma': 'https://gamma-api.polymarket.com/markets?limit=1',
        }

        for name, url in apis.items():
            try:
                response = urllib.request.urlopen(url, timeout=5)
                if response.status == 200:
                    print(f"✅ {name} API - reachable")
                else:
                    print(f"⚠️  {name} API - status {response.status}")
            except Exception as e:
                print(f"❌ {name} API - unreachable: {str(e)[:50]}")
                self.errors_found.append(f"{name} API unreachable")

    def generate_report(self):
        """Generate final report"""
        print("\n" + "=" * 60)
        print("📊 ERROR FIX REPORT")
        print("=" * 60)

        if self.errors_found:
            print("\n❌ Errors Found:")
            for error in self.errors_found:
                print(f"  - {error}")
        else:
            print("\n✅ No errors found!")

        if self.fixes_applied:
            print("\n✅ Fixes Applied:")
            for fix in self.fixes_applied:
                print(f"  - {fix}")

        print("\n" + "=" * 60)

        if not self.errors_found or len(self.errors_found) <= 2:
            print("🎉 System should be ready to run!")
            print("\nNext steps:")
            print("1. Run: python main.py")
            print("2. Access: http://localhost:8000")
        else:
            print("⚠️  Some issues remain. Please address manually.")
            print("\nSuggested actions:")
            if "API unreachable" in str(self.errors_found):
                print("- Check internet connection")
                print("- Try using a VPN if APIs are blocked")
            if "Import error" in str(self.errors_found):
                print("- Install missing packages with pip")
                print("- Check Python version (needs 3.8+)")


def main():
    print("=" * 60)
    print("🔧 Polymarket Whale Tracker - Error Diagnostic & Fix Tool")
    print("=" * 60)

    fixer = ErrorFixer()

    # Run all checks
    fixer.check_python_version()
    fixer.check_directory_structure()
    fixer.check_dependencies()
    fixer.fix_config_imports()
    fixer.create_env_file()
    fixer.test_imports()
    fixer.test_api_connectivity()

    # Generate report
    fixer.generate_report()


if __name__ == "__main__":
    main()