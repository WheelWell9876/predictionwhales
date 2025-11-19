"""
Configuration file for Polymarket Terminal
Contains API endpoints, database settings, and other configurations
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for Polymarket Terminal"""
    
    # Project root directory (parent of backend folder)
    PROJECT_ROOT = Path(__file__).parent.parent
    
    # Database Configuration - Database in project root
    DATABASE_PATH = os.getenv('DATABASE_PATH', str(PROJECT_ROOT / 'polymarket_terminal.db'))
    
    # Logging Configuration - Log in project root
    LOG_FILE = os.getenv('LOG_FILE', str(PROJECT_ROOT / 'polymarket_terminal.log'))
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # API Configuration
    GAMMA_API_URL = os.getenv('GAMMA_API_URL', 'https://gamma-api.polymarket.com')
    DATA_API_URL = os.getenv('DATA_API_URL', 'https://data-api.polymarket.com')
    CLOB_API_URL = os.getenv('CLOB_API_URL', 'https://clob.polymarket.com')
    
    # API Request Configuration
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 1  # seconds
    
    # Batch Processing Configuration
    BATCH_SIZE = 100
    COMMIT_INTERVAL = 50  # Commit to database every N records
    
    # Rate Limiting
    RATE_LIMIT_DELAY = 0.1  # seconds between API calls
    
    # Concurrency Configuration
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '20'))  # Max concurrent threads
    
    # WebSocket Configuration (for future use)
    WS_URL = os.getenv('WS_URL', 'wss://ws.polymarket.com')
    WS_RECONNECT_INTERVAL = 5  # seconds
    
    # Scheduled Updates Configuration
    ENABLE_DAILY_SCAN = os.getenv('ENABLE_DAILY_SCAN', 'true').lower() == 'true'
    DAILY_SCAN_TIME = os.getenv('DAILY_SCAN_TIME', '02:00')  # 2 AM
    FETCH_INTERVAL_MINUTES = int(os.getenv('FETCH_INTERVAL_MINUTES', '60'))  # For continuous mode
    
    # Data Fetching Configuration
    FETCH_CLOSED_EVENTS = os.getenv('FETCH_CLOSED_EVENTS', 'false').lower() == 'true'
    FETCH_ARCHIVED = os.getenv('FETCH_ARCHIVED', 'false').lower() == 'true'
    MAX_EVENTS_PER_RUN = int(os.getenv('MAX_EVENTS_PER_RUN', '1000'))
    MAX_MARKETS_PER_EVENT = int(os.getenv('MAX_MARKETS_PER_EVENT', '100'))
    
    # Comments Configuration
    MAX_COMMENTS_PER_EVENT = int(os.getenv('MAX_COMMENTS_PER_EVENT', '50'))
    FETCH_COMMENT_REACTIONS = os.getenv('FETCH_COMMENT_REACTIONS', 'true').lower() == 'true'
    
    # Feature Flags - All enabled by default
    FETCH_EVENTS = os.getenv('FETCH_EVENTS', 'true').lower() == 'true'
    FETCH_MARKETS = os.getenv('FETCH_MARKETS', 'true').lower() == 'true'
    FETCH_SERIES = os.getenv('FETCH_SERIES', 'true').lower() == 'true'
    FETCH_TAGS = os.getenv('FETCH_TAGS', 'true').lower() == 'true'
    FETCH_USERS = os.getenv('FETCH_USERS', 'true').lower() == 'true'
    FETCH_COMMENTS = os.getenv('FETCH_COMMENTS', 'true').lower() == 'true'
    FETCH_POSITIONS = os.getenv('FETCH_POSITIONS', 'true').lower() == 'true'
    FETCH_TRANSACTIONS = os.getenv('FETCH_TRANSACTIONS', 'true').lower() == 'true'
    
    # Data Resolution
    FETCH_LIVE_VOLUME = os.getenv('FETCH_LIVE_VOLUME', 'true').lower() == 'true'
    FETCH_OPEN_INTEREST = os.getenv('FETCH_OPEN_INTEREST', 'true').lower() == 'true'
    FETCH_DETAILED_INFO = os.getenv('FETCH_DETAILED_INFO', 'true').lower() == 'true'
    
    # Whale Tracking Configuration
    MIN_TRANSACTION_SIZE = float(os.getenv('MIN_TRANSACTION_SIZE', '500'))  # $500 minimum transaction
    MIN_WHALE_WALLET = float(os.getenv('MIN_WHALE_WALLET', '10000'))  # $10k minimum wallet
    MIN_WHALE_TRADE = float(os.getenv('MIN_WHALE_TRADE', '10000'))  # $10k minimum trade
    MIN_POSITION_VALUE = float(os.getenv('MIN_POSITION_VALUE', '500'))  # $500 minimum position
    
    # User and Transaction Limits
    INITIAL_USERS_PER_EVENT = int(os.getenv('INITIAL_USERS_PER_EVENT', '100'))
    INITIAL_TRANSACTIONS_PER_USER = int(os.getenv('INITIAL_TRANSACTIONS_PER_USER', '20'))
    MAX_TRACKED_WALLETS = int(os.getenv('MAX_TRACKED_WALLETS', '1000'))
    MAX_RECENT_TRADES = int(os.getenv('MAX_RECENT_TRADES', '500'))
    
    # Legacy compatibility  
    MIN_BET_AMOUNT = MIN_TRANSACTION_SIZE  # Alias for backward compatibility
    MIN_WHALE_VOLUME = MIN_WHALE_TRADE  # Alias for backward compatibility
    UPDATE_INTERVAL = FETCH_INTERVAL_MINUTES * 60  # Convert to seconds
    MAX_RECENT_BETS = MAX_RECENT_TRADES  # Alias for backward compatibility
    
    @classmethod
    def get_api_headers(cls):
        """Get headers for API requests"""
        return {
            'Accept': 'application/json',
            'User-Agent': 'Polymarket-Terminal/2.0',
            'Content-Type': 'application/json'
        }
    
    @classmethod
    def get_database_url(cls):
        """Get full database URL"""
        return f'sqlite:///{cls.DATABASE_PATH}'
    
    @classmethod
    def get_database_path(cls):
        """Get database path as Path object"""
        return Path(cls.DATABASE_PATH)
    
    @classmethod
    def get_log_path(cls):
        """Get log file path as Path object"""
        return Path(cls.LOG_FILE)
    
    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist"""
        # Database and log files are in project root, no need to create directories
        # But ensure the parent directory exists
        db_path = cls.get_database_path()
        log_path = cls.get_log_path()
        
        # These should be in project root, but just in case
        db_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.parent.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate_config(cls):
        """Validate configuration settings"""
        issues = []
        
        # Check if database path is accessible
        try:
            db_path = cls.get_database_path()
            if db_path.exists() and not os.access(str(db_path), os.W_OK):
                issues.append(f"Database file is not writable: {db_path}")
        except Exception as e:
            issues.append(f"Error checking database path: {e}")
        
        # Check API URLs
        if not cls.GAMMA_API_URL:
            issues.append("GAMMA_API_URL is not configured")
        if not cls.DATA_API_URL:
            issues.append("DATA_API_URL is not configured")
        if not cls.CLOB_API_URL:
            issues.append("CLOB_API_URL is not configured")
        
        # Check whale thresholds
        if cls.MIN_WHALE_WALLET <= 0:
            issues.append("MIN_WHALE_WALLET must be positive")
        if cls.MIN_WHALE_TRADE <= 0:
            issues.append("MIN_WHALE_TRADE must be positive")
        
        return issues
    
    @classmethod
    def print_config(cls):
        """Print current configuration for debugging"""
        print("\n" + "=" * 60)
        print("POLYMARKET TERMINAL CONFIGURATION")
        print("=" * 60)
        print(f"Database: {cls.DATABASE_PATH}")
        print(f"Log File: {cls.LOG_FILE}")
        print(f"API URLs:")
        print(f"  - Gamma: {cls.GAMMA_API_URL}")
        print(f"  - Data: {cls.DATA_API_URL}")
        print(f"  - CLOB: {cls.CLOB_API_URL}")
        print(f"Whale Thresholds:")
        print(f"  - Min Transaction: ${cls.MIN_TRANSACTION_SIZE:,.0f}")
        print(f"  - Min Whale Wallet: ${cls.MIN_WHALE_WALLET:,.0f}")
        print(f"  - Min Whale Trade: ${cls.MIN_WHALE_TRADE:,.0f}")
        print(f"Features Enabled:")
        for feature in ['EVENTS', 'MARKETS', 'SERIES', 'TAGS', 'USERS', 
                       'COMMENTS', 'POSITIONS', 'TRANSACTIONS']:
            enabled = getattr(cls, f'FETCH_{feature}')
            status = "✅" if enabled else "❌"
            print(f"  - {feature}: {status}")
        print("=" * 60)