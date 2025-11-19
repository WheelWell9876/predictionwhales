"""
Backend package for Polymarket Terminal
Version 2.0 - Refactored Architecture
"""

__version__ = '2.0.0'
__author__ = 'Polymarket Terminal Team'

# Lazy imports to avoid circular dependencies
def get_config():
    """Get configuration class"""
    from .config import Config
    return Config

def get_database_manager():
    """Get database manager"""
    from .database.database_manager import DatabaseManager
    return DatabaseManager()

def get_data_fetcher():
    """Get main data fetcher orchestrator"""
    from .database.data_fetcher import PolymarketDataFetcher
    return PolymarketDataFetcher()

# Entity Managers
def get_events_manager():
    """Get events manager"""
    from .events_manager import EventsManager
    return EventsManager()

def get_markets_manager():
    """Get markets manager"""
    from .markets_manager import MarketsManager
    return MarketsManager()

def get_series_manager():
    """Get series manager"""
    from .series_manager import SeriesManager
    return SeriesManager()

def get_tags_manager():
    """Get tags manager"""
    from .tags_manager import TagsManager
    return TagsManager()

def get_users_manager():
    """Get users manager"""
    from .users_manager import UsersManager
    return UsersManager()

def get_comments_manager():
    """Get comments manager"""
    from .comments_manager import CommentsManager
    return CommentsManager()

def get_positions_manager():
    """Get positions manager"""
    from .positions_manager import PositionsManager
    return PositionsManager()

def get_transactions_manager():
    """Get transactions manager"""
    from .transactions_manager import TransactionsManager
    return TransactionsManager()

# Utility functions
def initialize_database():
    """Initialize the database schema"""
    from .database.database_manager import DatabaseManager
    db = DatabaseManager()
    return db

def get_database_status():
    """Get current database status"""
    from .database.db_utils import check_database_status
    check_database_status()

def validate_configuration():
    """Validate current configuration"""
    from .config import Config
    issues = Config.validate_config()
    if issues:
        print("⚠️ Configuration Issues:")
        for issue in issues:
            print(f"  • {issue}")
        return False
    else:
        print("✅ Configuration is valid")
        return True

def print_configuration():
    """Print current configuration"""
    from .config import Config
    Config.print_config()

# Quick access to common operations
class QuickOps:
    """Quick access to common operations"""
    
    @staticmethod
    def load_events():
        """Quick load events"""
        fetcher = get_data_fetcher()
        return fetcher.load_events_only()
    
    @staticmethod
    def load_markets():
        """Quick load markets"""
        fetcher = get_data_fetcher()
        return fetcher.load_markets_only()
    
    @staticmethod
    def load_positions():
        """Quick load positions"""
        fetcher = get_data_fetcher()
        return fetcher.load_positions_only()
    
    @staticmethod
    def load_all():
        """Quick load all data"""
        fetcher = get_data_fetcher()
        return fetcher.run_daily_scan()
    
    @staticmethod
    def get_stats():
        """Get database statistics"""
        db = get_database_manager()
        stats = {}
        
        tables = [
            'events', 'markets', 'series', 'tags', 
            'users', 'comments', 'transactions',
            'user_positions_current', 'user_positions_closed'
        ]
        
        for table in tables:
            try:
                count = db.get_table_count(table)
                stats[table] = count
            except:
                stats[table] = 0
        
        return stats

# Package information
def get_info():
    """Get package information"""
    info = {
        'version': __version__,
        'author': __author__,
        'managers': [
            'EventsManager', 'MarketsManager', 'SeriesManager', 
            'TagsManager', 'UsersManager', 'CommentsManager',
            'PositionsManager', 'TransactionsManager'
        ],
        'features': {
            'events': 'Event data fetching and storage',
            'markets': 'Market data with outcome prices',
            'series': 'Series and collections management',
            'tags': 'Tag relationships and categorization',
            'users': 'Whale user tracking and profiling',
            'comments': 'Event comments and reactions',
            'positions': 'Current and closed position tracking',
            'transactions': 'Transaction and trading activity'
        }
    }
    return info

# Export main classes for convenience
__all__ = [
    'get_config',
    'get_database_manager',
    'get_data_fetcher',
    'get_events_manager',
    'get_markets_manager',
    'get_series_manager',
    'get_tags_manager',
    'get_users_manager',
    'get_comments_manager',
    'get_positions_manager',
    'get_transactions_manager',
    'initialize_database',
    'get_database_status',
    'validate_configuration',
    'print_configuration',
    'QuickOps',
    'get_info',
    '__version__'
]