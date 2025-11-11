"""
Complete Database Schema for Polymarket Terminal
Includes all tables for events, markets, users, transactions, and relationships
"""

import sqlite3
import os
from datetime import datetime

def create_complete_schema(db_path='polymarket_terminal.db'):
    """Create all database tables with proper schema"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")

    print("Creating database schema...")

    # ================== CORE TABLES ==================

    # Events table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        description TEXT,
        start_date TIMESTAMP,
        creation_date TIMESTAMP,
        end_date TIMESTAMP,
        image TEXT,
        icon TEXT,
        liquidity REAL,
        liquidity_clob REAL,
        volume REAL,
        volume_clob REAL,
        volume_24hr REAL,
        volume_24hr_clob REAL,
        volume_1wk REAL,
        volume_1wk_clob REAL,
        volume_1mo REAL,
        volume_1mo_clob REAL,
        volume_1yr REAL,
        volume_1yr_clob REAL,
        open_interest REAL,
        competitive REAL,
        comment_count INTEGER,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        enable_order_book INTEGER DEFAULT 1,
        cyom INTEGER DEFAULT 0,
        show_all_outcomes INTEGER DEFAULT 1,
        show_market_images INTEGER DEFAULT 1,
        enable_neg_risk INTEGER DEFAULT 0,
        automatically_active INTEGER DEFAULT 1,
        neg_risk_augmented INTEGER DEFAULT 0,
        pending_deployment INTEGER DEFAULT 0,
        deploying INTEGER DEFAULT 0,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Markets table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY,
        event_id TEXT,
        question TEXT,
        condition_id TEXT UNIQUE,
        slug TEXT,
        resolution_source TEXT,
        description TEXT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        end_date_iso TEXT,
        start_date_iso TEXT,
        image TEXT,
        icon TEXT,
        outcomes TEXT,  -- JSON array
        outcome_prices TEXT,  -- JSON array
        clob_token_ids TEXT,  -- JSON array
        liquidity REAL,
        liquidity_num REAL,
        liquidity_clob REAL,
        volume REAL,
        volume_num REAL,
        volume_clob REAL,
        volume_24hr REAL,
        volume_24hr_clob REAL,
        volume_1wk REAL,
        volume_1wk_clob REAL,
        volume_1mo REAL,
        volume_1mo_clob REAL,
        volume_1yr REAL,
        volume_1yr_clob REAL,
        open_interest REAL,
        last_trade_price REAL,
        best_bid REAL,
        best_ask REAL,
        spread REAL,
        one_day_price_change REAL,
        one_week_price_change REAL,
        one_month_price_change REAL,
        competitive REAL,
        market_maker_address TEXT,
        submitted_by TEXT,
        resolved_by TEXT,
        question_id TEXT,
        group_item_title TEXT,
        group_item_threshold TEXT,
        enable_order_book INTEGER DEFAULT 1,
        order_price_min_tick_size REAL,
        order_min_size REAL,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        ready INTEGER DEFAULT 0,
        funded INTEGER DEFAULT 0,
        neg_risk INTEGER DEFAULT 0,
        neg_risk_other INTEGER DEFAULT 0,
        cyom INTEGER DEFAULT 0,
        has_reviewed_dates INTEGER DEFAULT 0,
        accepting_orders INTEGER DEFAULT 1,
        accepting_orders_timestamp TIMESTAMP,
        automatically_active INTEGER DEFAULT 1,
        clear_book_on_start INTEGER DEFAULT 0,
        manual_activation INTEGER DEFAULT 0,
        pending_deployment INTEGER DEFAULT 0,
        deploying INTEGER DEFAULT 0,
        rfq_enabled INTEGER DEFAULT 0,
        holding_rewards_enabled INTEGER DEFAULT 0,
        fees_enabled INTEGER DEFAULT 0,
        pager_duty_notification_enabled INTEGER DEFAULT 0,
        approved INTEGER DEFAULT 1,
        rewards_min_size REAL,
        rewards_max_spread REAL,
        uma_bond REAL,
        uma_reward REAL,
        uma_resolution_statuses TEXT,  -- JSON array
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (event_id) REFERENCES events(id)
    )""")

    # Tags table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY,
        label TEXT,
        slug TEXT UNIQUE,
        force_show INTEGER DEFAULT 0,
        force_hide INTEGER DEFAULT 0,
        is_carousel INTEGER DEFAULT 0,
        published_at TIMESTAMP,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Series table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        subtitle TEXT,
        series_type TEXT,
        recurrence TEXT,
        description TEXT,
        image TEXT,
        icon TEXT,
        layout TEXT,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        is_template INTEGER DEFAULT 0,
        template_variables TEXT,  -- JSON
        published_at TIMESTAMP,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        comments_enabled INTEGER DEFAULT 1,
        competitive REAL,
        volume_24hr REAL,
        volume REAL,
        liquidity REAL,
        start_date TIMESTAMP,
        pyth_token_id TEXT,
        cg_asset_name TEXT,
        score REAL,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # Collections table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collections (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        subtitle TEXT,
        collection_type TEXT,
        description TEXT,
        tags TEXT,  -- JSON array
        image TEXT,
        icon TEXT,
        header_image TEXT,
        layout TEXT,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        is_template INTEGER DEFAULT 0,
        template_variables TEXT,  -- JSON
        published_at TIMESTAMP,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        comments_enabled INTEGER DEFAULT 1,
        comment_count INTEGER,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # ================== USER TABLES ==================

    # Users table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        proxy_wallet TEXT PRIMARY KEY,
        username TEXT,
        pseudonym TEXT,
        bio TEXT,
        profile_image TEXT,
        profile_image_optimized TEXT,
        total_value REAL,
        total_pnl REAL,
        markets_traded INTEGER DEFAULT 0,
        is_whale INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")

    # User current positions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_positions_current (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        asset TEXT,
        condition_id TEXT,
        size REAL,
        avg_price REAL,
        initial_value REAL,
        current_value REAL,
        cash_pnl REAL,
        percent_pnl REAL,
        total_bought REAL,
        realized_pnl REAL,
        percent_realized_pnl REAL,
        cur_price REAL,
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
        end_date TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        FOREIGN KEY (event_id) REFERENCES events(id),
        UNIQUE(proxy_wallet, asset, condition_id)
    )""")

    # User closed positions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_positions_closed (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        asset TEXT,
        condition_id TEXT,
        avg_price REAL,
        total_bought REAL,
        realized_pnl REAL,
        cur_price REAL,
        title TEXT,
        slug TEXT,
        icon TEXT,
        event_slug TEXT,
        outcome TEXT,
        outcome_index INTEGER,
        opposite_outcome TEXT,
        opposite_asset TEXT,
        end_date TIMESTAMP,
        closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        UNIQUE(proxy_wallet, asset, condition_id)
    )""")

    # User trades
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        side TEXT,  -- BUY or SELL
        asset TEXT,
        condition_id TEXT,
        size REAL,
        price REAL,
        timestamp TIMESTAMP,
        transaction_hash TEXT UNIQUE,
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
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")

    # User activity
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        timestamp TIMESTAMP,
        condition_id TEXT,
        transaction_hash TEXT UNIQUE,
        type TEXT,  -- TRADE, DEPOSIT, WITHDRAW, etc.
        side TEXT,  -- BUY, SELL
        size REAL,
        usdc_size REAL,
        price REAL,
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
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")

    # User portfolio values
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        market_condition_id TEXT,  -- NULL for total portfolio value
        value REAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")

    # Market holders
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_holders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_id TEXT,
        token_id TEXT,
        proxy_wallet TEXT,
        username TEXT,
        pseudonym TEXT,
        amount REAL,
        outcome_index INTEGER,
        bio TEXT,
        profile_image TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (market_id) REFERENCES markets(id),
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        UNIQUE(market_id, token_id, proxy_wallet)
    )""")

    # ================== TRANSACTION TABLES ==================

    # Transactions table (from CLOB API)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        proxy_wallet TEXT,
        username TEXT,
        condition_id TEXT,
        time_created TIMESTAMP,
        usdc_size REAL,
        shares_count REAL,
        avg_price REAL,
        side TEXT,
        type TEXT,
        market_id TEXT,
        transaction_hash TEXT,
        outcome TEXT,
        outcome_index INTEGER,
        is_whale INTEGER DEFAULT 0,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        FOREIGN KEY (market_id) REFERENCES markets(id)
    )""")

    # Comments table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        event_id TEXT,
        market_id TEXT,
        proxy_wallet TEXT,
        username TEXT,
        profile_image TEXT,
        content TEXT,
        parent_comment_id TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        likes_count INTEGER DEFAULT 0,
        replies_count INTEGER DEFAULT 0,
        FOREIGN KEY (event_id) REFERENCES events(id),
        FOREIGN KEY (market_id) REFERENCES markets(id),
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        FOREIGN KEY (parent_comment_id) REFERENCES comments(id)
    )""")

    # Comment reactions
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment_reactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comment_id TEXT,
        proxy_wallet TEXT,
        reaction_type TEXT,  -- LIKE, DISLIKE, etc.
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (comment_id) REFERENCES comments(id),
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        UNIQUE(comment_id, proxy_wallet, reaction_type)
    )""")

    # ================== RELATIONSHIP TABLES ==================

    # Event-Tags relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_tags (
        tag_id INTEGER PRIMARY KEY,
        event_ids TEXT NOT NULL,
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")

    # Market-Tags relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_tags (
        market_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (market_id, tag_id),
        FOREIGN KEY (market_id) REFERENCES markets(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")

    # Series-Events relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_events (
        series_id TEXT PRIMARY KEY,
        event_ids TEXT NOT NULL,
        FOREIGN KEY (series_id) REFERENCES series(id)
    )""")

    # Series-Collections relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_collections (
        series_id TEXT,
        collection_id TEXT,
        PRIMARY KEY (series_id, collection_id),
        FOREIGN KEY (series_id) REFERENCES series(id),
        FOREIGN KEY (collection_id) REFERENCES collections(id)
    )""")

    # Series-Tags relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_tags (
        series_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (series_id, tag_id),
        FOREIGN KEY (series_id) REFERENCES series(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")

    # Collection-Tags relationship
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collection_tags (
        collection_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (collection_id, tag_id),
        FOREIGN KEY (collection_id) REFERENCES collections(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")

    # Tag relationships
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tag_relationships (
        id TEXT PRIMARY KEY,
        tag_id INTEGER,
        related_tag_id INTEGER,
        rank INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (tag_id) REFERENCES tags(id),
        FOREIGN KEY (related_tag_id) REFERENCES tags(id),
        UNIQUE(tag_id, related_tag_id)
    )""")

    # ================== ANALYTICS TABLES ==================

    # Event live volume
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_live_volume (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        total_volume REAL,
        market_volumes TEXT,  -- JSON array
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (event_id) REFERENCES events(id)
    )""")

    # Market open interest
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_open_interest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_id TEXT,
        condition_id TEXT,
        open_interest REAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (market_id) REFERENCES markets(id)
    )""")

    # ================== CREATE INDEXES ==================

    print("Creating indexes...")

    # Events indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_slug ON events(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_active ON events(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC)")

    # Markets indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume DESC)")

    # Users indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_value ON users(total_value DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_whale ON users(is_whale)")

    # Positions indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_wallet ON user_positions_current(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_value ON user_positions_current(current_value DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_condition ON user_positions_current(condition_id)")

    # Trades indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_wallet ON user_trades(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON user_trades(timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_condition ON user_trades(condition_id)")

    # Activity indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_activity_wallet ON user_activity(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON user_activity(timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_activity_condition ON user_activity(condition_id)")

    # Transactions indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions(time_created DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_whale ON transactions(is_whale)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_condition ON transactions(condition_id)")

    # Market holders indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_holders_market ON market_holders(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_holders_wallet ON market_holders(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_holders_amount ON market_holders(amount DESC)")

    # Values indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_values_wallet ON user_values(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_values_timestamp ON user_values(timestamp DESC)")

    conn.commit()
    conn.close()

    print("✅ Database schema created successfully with all tables!")
    return True

if __name__ == "__main__":
    create_complete_schema()