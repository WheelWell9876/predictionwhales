"""
Database Schema for Polymarket Terminal
Complete schema definition for all tables
"""

SCHEMA = """
-- Events table
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    slug TEXT UNIQUE,
    title TEXT,
    description TEXT,
    start_date TEXT,
    end_date TEXT,
    icon TEXT,
    active INTEGER DEFAULT 1,
    closed INTEGER DEFAULT 0,
    archived INTEGER DEFAULT 0,
    new INTEGER DEFAULT 0,
    featured INTEGER DEFAULT 0,
    restricted INTEGER DEFAULT 0,
    liquidity REAL DEFAULT 0,
    volume REAL DEFAULT 0,
    volume_24hr REAL DEFAULT 0,
    competitive INTEGER DEFAULT 0,
    market_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT
);

-- Markets table
CREATE TABLE IF NOT EXISTS markets (
    id TEXT PRIMARY KEY,
    event_id TEXT,
    condition_id TEXT UNIQUE,
    question TEXT,
    description TEXT,
    market_type TEXT,
    outcome_prices TEXT,
    outcome_count INTEGER,
    active INTEGER DEFAULT 1,
    closed INTEGER DEFAULT 0,
    liquidity REAL DEFAULT 0,
    volume REAL DEFAULT 0,
    volume_24hr REAL DEFAULT 0,
    yes_price REAL,
    no_price REAL,
    last_trade_price REAL,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

-- Live volume tracking table
CREATE TABLE IF NOT EXISTS event_live_volume (
    event_id TEXT,
    timestamp TEXT,
    volume REAL,
    volume_24hr REAL,
    PRIMARY KEY (event_id, timestamp),
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

-- Open interest tracking table
CREATE TABLE IF NOT EXISTS market_open_interest (
    market_id TEXT,
    timestamp TEXT,
    open_interest REAL,
    PRIMARY KEY (market_id, timestamp),
    FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE
);

-- Collections table
CREATE TABLE IF NOT EXISTS collections (
    id TEXT PRIMARY KEY,
    slug TEXT UNIQUE,
    title TEXT,
    description TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- Series table
CREATE TABLE IF NOT EXISTS series (
    id TEXT PRIMARY KEY,
    slug TEXT UNIQUE,
    title TEXT,
    description TEXT,
    creator TEXT,
    liquidity REAL DEFAULT 0,
    volume REAL DEFAULT 0,
    volume_24hr REAL DEFAULT 0,
    volume_1wk REAL DEFAULT 0,
    volume_1mo REAL DEFAULT 0,
    volume_1yr REAL DEFAULT 0,
    open_interest REAL DEFAULT 0,
    avg_price REAL DEFAULT 0,
    active INTEGER DEFAULT 1,
    closed INTEGER DEFAULT 0,
    archived INTEGER DEFAULT 0,
    new INTEGER DEFAULT 0,
    featured INTEGER DEFAULT 0,
    restricted INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT
);

-- Series-Events relationship table
CREATE TABLE IF NOT EXISTS series_events (
    series_id TEXT,
    event_id TEXT,
    position INTEGER DEFAULT 0,
    created_at TEXT,
    PRIMARY KEY (series_id, event_id),
    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

-- Series-Collections relationship table
CREATE TABLE IF NOT EXISTS series_collections (
    series_id TEXT,
    collection_id TEXT,
    collection_title TEXT,
    position INTEGER DEFAULT 0,
    created_at TEXT,
    PRIMARY KEY (series_id, collection_id),
    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
);

-- Tags table
CREATE TABLE IF NOT EXISTS tags (
    id TEXT PRIMARY KEY,
    label TEXT,
    slug TEXT UNIQUE,
    force_show INTEGER DEFAULT 0,
    force_hide INTEGER DEFAULT 0,
    is_carousel INTEGER DEFAULT 0,
    published_at TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT
);

-- Tag relationships table
CREATE TABLE IF NOT EXISTS tag_relationships (
    tag_id TEXT,
    related_tag_id TEXT,
    relationship_type TEXT,
    strength REAL DEFAULT 1.0,
    created_at TEXT,
    PRIMARY KEY (tag_id, related_tag_id),
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Event-Tags relationship table (CRITICAL - was missing!)
CREATE TABLE IF NOT EXISTS event_tags (
    event_id TEXT,
    tag_id TEXT,
    tag_slug TEXT,
    PRIMARY KEY (event_id, tag_id),
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Market-Tags relationship table
CREATE TABLE IF NOT EXISTS market_tags (
    market_id TEXT,
    tag_id TEXT,
    tag_slug TEXT,
    PRIMARY KEY (market_id, tag_id),
    FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Series-Tags relationship table
CREATE TABLE IF NOT EXISTS series_tags (
    series_id TEXT,
    tag_id TEXT,
    tag_slug TEXT,
    PRIMARY KEY (series_id, tag_id),
    FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Collection-Tags relationship table
CREATE TABLE IF NOT EXISTS collection_tags (
    collection_id TEXT,
    tag_id TEXT,
    tag_slug TEXT,
    PRIMARY KEY (collection_id, tag_id),
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);

-- Users table (whale focused)
CREATE TABLE IF NOT EXISTS users (
    proxy_wallet TEXT PRIMARY KEY,
    username TEXT,
    pseudonym TEXT,
    bio TEXT,
    profile_image TEXT,
    profile_image_optimized TEXT,
    total_value REAL DEFAULT 0,
    is_whale INTEGER DEFAULT 0,
    last_updated TEXT,
    created_at TEXT
);

-- Market holders table
CREATE TABLE IF NOT EXISTS market_holders (
    market_id TEXT,
    token_id TEXT,
    proxy_wallet TEXT,
    username TEXT,
    pseudonym TEXT,
    amount REAL DEFAULT 0,
    outcome_index INTEGER,
    bio TEXT,
    profile_image TEXT,
    updated_at TEXT,
    PRIMARY KEY (market_id, token_id, proxy_wallet),
    FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE,
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- User activity table
CREATE TABLE IF NOT EXISTS user_activity (
    proxy_wallet TEXT,
    timestamp TEXT,
    condition_id TEXT,
    transaction_hash TEXT,
    type TEXT,
    side TEXT,
    size REAL DEFAULT 0,
    usdc_size REAL DEFAULT 0,
    price REAL DEFAULT 0,
    asset TEXT,
    outcome_index INTEGER,
    title TEXT,
    slug TEXT,
    event_slug TEXT,
    outcome TEXT,
    username TEXT,
    pseudonym TEXT,
    bio TEXT,
    profile_image TEXT,
    PRIMARY KEY (proxy_wallet, transaction_hash),
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- User trades table
CREATE TABLE IF NOT EXISTS user_trades (
    proxy_wallet TEXT,
    side TEXT,
    asset TEXT,
    condition_id TEXT,
    size REAL DEFAULT 0,
    price REAL DEFAULT 0,
    timestamp TEXT,
    transaction_hash TEXT PRIMARY KEY,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INTEGER,
    username TEXT,
    pseudonym TEXT,
    bio TEXT,
    profile_image TEXT,
    profile_image_optimized TEXT,
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- User current positions table
CREATE TABLE IF NOT EXISTS user_positions_current (
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    size REAL DEFAULT 0,
    avg_price REAL DEFAULT 0,
    initial_value REAL DEFAULT 0,
    current_value REAL DEFAULT 0,
    cash_pnl REAL DEFAULT 0,
    percent_pnl REAL DEFAULT 0,
    total_bought REAL DEFAULT 0,
    realized_pnl REAL DEFAULT 0,
    percent_realized_pnl REAL DEFAULT 0,
    cur_price REAL DEFAULT 0,
    redeemable INTEGER DEFAULT 0,
    mergeable INTEGER DEFAULT 0,
    negative_risk INTEGER DEFAULT 0,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_id TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INTEGER,
    opposite_outcome TEXT,
    opposite_asset TEXT,
    end_date TEXT,
    updated_at TEXT,
    PRIMARY KEY (proxy_wallet, asset),
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- User closed positions table
CREATE TABLE IF NOT EXISTS user_positions_closed (
    proxy_wallet TEXT,
    asset TEXT,
    condition_id TEXT,
    avg_price REAL DEFAULT 0,
    total_bought REAL DEFAULT 0,
    realized_pnl REAL DEFAULT 0,
    cur_price REAL DEFAULT 0,
    title TEXT,
    slug TEXT,
    icon TEXT,
    event_slug TEXT,
    outcome TEXT,
    outcome_index INTEGER,
    opposite_outcome TEXT,
    opposite_asset TEXT,
    end_date TEXT,
    closed_at TEXT,
    PRIMARY KEY (proxy_wallet, asset, closed_at),
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- User values table
CREATE TABLE IF NOT EXISTS user_values (
    proxy_wallet TEXT,
    market_condition_id TEXT,
    value REAL DEFAULT 0,
    PRIMARY KEY (proxy_wallet, COALESCE(market_condition_id, '')),
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE
);

-- Transactions table (whale focused)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_hash TEXT PRIMARY KEY,
    proxy_wallet TEXT,
    timestamp TEXT,
    market_id TEXT,
    condition_id TEXT,
    side TEXT,
    size REAL DEFAULT 0,
    price REAL DEFAULT 0,
    usdc_size REAL DEFAULT 0,
    type TEXT DEFAULT 'trade',
    username TEXT,
    pseudonym TEXT,
    is_whale INTEGER DEFAULT 0,
    FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet) ON DELETE CASCADE,
    FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE
);

-- Comments table
CREATE TABLE IF NOT EXISTS comments (
    id TEXT PRIMARY KEY,
    event_id TEXT,
    parent_id TEXT,
    content TEXT,
    user_id TEXT,
    username TEXT,
    pseudonym TEXT,
    user_profile_image TEXT,
    likes_count INTEGER DEFAULT 0,
    dislikes_count INTEGER DEFAULT 0,
    replies_count INTEGER DEFAULT 0,
    is_pinned INTEGER DEFAULT 0,
    is_edited INTEGER DEFAULT 0,
    created_at TEXT,
    updated_at TEXT,
    fetched_at TEXT,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);

-- Comment reactions table
CREATE TABLE IF NOT EXISTS comment_reactions (
    comment_id TEXT,
    user_id TEXT,
    username TEXT,
    reaction_type TEXT,
    created_at TEXT,
    PRIMARY KEY (comment_id, user_id, reaction_type),
    FOREIGN KEY (comment_id) REFERENCES comments(id) ON DELETE CASCADE
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_events_closed ON events(closed);
CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC);
CREATE INDEX IF NOT EXISTS idx_events_updated ON events(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);
CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume DESC);
CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active);

CREATE INDEX IF NOT EXISTS idx_series_volume ON series(volume DESC);
CREATE INDEX IF NOT EXISTS idx_series_slug ON series(slug);

CREATE INDEX IF NOT EXISTS idx_tags_slug ON tags(slug);

-- Indexes for relationship tables
CREATE INDEX IF NOT EXISTS idx_event_tags_event ON event_tags(event_id);
CREATE INDEX IF NOT EXISTS idx_event_tags_tag ON event_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_market_tags_market ON market_tags(market_id);
CREATE INDEX IF NOT EXISTS idx_market_tags_tag ON market_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_series_tags_series ON series_tags(series_id);
CREATE INDEX IF NOT EXISTS idx_series_tags_tag ON series_tags(tag_id);

CREATE INDEX IF NOT EXISTS idx_users_whale ON users(is_whale);
CREATE INDEX IF NOT EXISTS idx_users_value ON users(total_value DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_size ON transactions(usdc_size DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_whale ON transactions(is_whale);

CREATE INDEX IF NOT EXISTS idx_activity_wallet ON user_activity(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON user_activity(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_activity_size ON user_activity(usdc_size DESC);

CREATE INDEX IF NOT EXISTS idx_trades_wallet ON user_trades(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON user_trades(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_positions_wallet ON user_positions_current(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_positions_value ON user_positions_current(current_value DESC);

CREATE INDEX IF NOT EXISTS idx_closed_positions_wallet ON user_positions_closed(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_closed_positions_pnl ON user_positions_closed(realized_pnl DESC);

CREATE INDEX IF NOT EXISTS idx_comments_event ON comments(event_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_at DESC);

-- Enable foreign key constraints
PRAGMA foreign_keys = ON;

-- Set optimal pragmas for performance
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;
PRAGMA temp_store = MEMORY;
"""

def get_schema():
    """Get the database schema SQL"""
    return SCHEMA