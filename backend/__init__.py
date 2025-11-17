"""
Backend package for Polymarket Terminal
"""

__version__ = '1.0.0'

# Lazy imports to avoid circular dependencies
def get_config():
    from .config import Config
    return Config

def get_database_manager():
    from .database.database_manager import DatabaseManager
    return DatabaseManager

def get_events_manager():
    from .events_manager import EventsManager
    return EventsManager

def get_markets_manager():
    from .markets_manager import MarketsManager
    return MarketsManager

def get_series_manager():
    from .series_manager import SeriesManager
    return SeriesManager

def get_tags_manager():
    from .tags_manager import TagsManager
    return TagsManager

def get_data_fetcher():
    from .database.data_fetcher import PolymarketDataFetcher
    return PolymarketDataFetcher
