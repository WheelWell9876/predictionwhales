"""
Configuration file for Polymarket data fetcher
Contains API endpoints, database settings, and other configurations
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration class for Polymarket data fetcher"""

    # Database Configuration
    DATABASE_PATH = os.getenv('DATABASE_PATH', 'polymarket_terminal.db')

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

    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'polymarket_fetcher.log')

    # WebSocket Configuration (for future use)
    WS_URL = os.getenv('WS_URL', 'wss://ws.polymarket.com')
    WS_RECONNECT_INTERVAL = 5  # seconds

    # Cron Job Configuration
    ENABLE_DAILY_SCAN = os.getenv('ENABLE_DAILY_SCAN', 'true').lower() == 'true'
    DAILY_SCAN_TIME = os.getenv('DAILY_SCAN_TIME', '02:00')  # 2 AM

    # Data Fetching Configuration
    FETCH_CLOSED_EVENTS = os.getenv('FETCH_CLOSED_EVENTS', 'false').lower() == 'true'
    FETCH_ARCHIVED = os.getenv('FETCH_ARCHIVED', 'false').lower() == 'true'
    MAX_EVENTS_PER_RUN = int(os.getenv('MAX_EVENTS_PER_RUN', '1000'))
    MAX_MARKETS_PER_EVENT = int(os.getenv('MAX_MARKETS_PER_EVENT', '100'))

    # Feature Flags
    FETCH_LIVE_VOLUME = os.getenv('FETCH_LIVE_VOLUME', 'true').lower() == 'true'
    FETCH_OPEN_INTEREST = os.getenv('FETCH_OPEN_INTEREST', 'true').lower() == 'true'
    FETCH_SERIES = os.getenv('FETCH_SERIES', 'true').lower() == 'true'
    FETCH_TAGS = os.getenv('FETCH_TAGS', 'true').lower() == 'true'

    @classmethod
    def get_api_headers(cls):
        """Get headers for API requests"""
        return {
            'Accept': 'application/json',
            'User-Agent': 'Polymarket-Terminal/1.0'
        }

    @classmethod
    def get_database_url(cls):
        """Get full database URL"""
        return f'sqlite:///{cls.DATABASE_PATH}'