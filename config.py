from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings"""

    # API Configuration
    POLYMARKET_API_KEY: Optional[str] = None  # Add if you have an API key

    # Tracking Configuration
    MIN_BET_AMOUNT: float = 1000  # Minimum USD value to consider a "large bet"
    MIN_WHALE_VOLUME: float = 10000  # Minimum total volume to be considered a whale
    UPDATE_INTERVAL: int = 300  # Update interval in seconds (5 minutes)

    # Database Configuration (if you want to add persistence later)
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


settings = Settings()