"""
Complete Database Schema for Polymarket Trading Terminal
This file creates all necessary tables for the trading interface
"""

import sqlite3
import os
from datetime import datetime

def create_complete_schema(db_path="polymarket_terminal.db"):
    """Create all database tables with proper schema"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # ============= EVENTS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        description TEXT,
        start_date TEXT,
        creation_date TEXT,
        end_date TEXT,
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
        active BOOLEAN,
        closed BOOLEAN,
        archived BOOLEAN,
        new BOOLEAN,
        featured BOOLEAN,
        restricted BOOLEAN,
        enable_order_book BOOLEAN,
        cyom BOOLEAN,
        show_all_outcomes BOOLEAN,
        show_market_images BOOLEAN,
        enable_neg_risk BOOLEAN,
        automatically_active BOOLEAN,
        neg_risk_augmented BOOLEAN,
        pending_deployment BOOLEAN,
        deploying BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= MARKETS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY,
        event_id TEXT,
        question TEXT,
        condition_id TEXT,
        slug TEXT,
        resolution_source TEXT,
        description TEXT,
        start_date TEXT,
        end_date TEXT,
        end_date_iso TEXT,
        start_date_iso TEXT,
        image TEXT,
        icon TEXT,
        outcomes TEXT,
        outcome_prices TEXT,
        clob_token_ids TEXT,
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
        enable_order_book BOOLEAN,
        order_price_min_tick_size REAL,
        order_min_size REAL,
        active BOOLEAN,
        closed BOOLEAN,
        archived BOOLEAN,
        new BOOLEAN,
        featured BOOLEAN,
        restricted BOOLEAN,
        ready BOOLEAN,
        funded BOOLEAN,
        neg_risk BOOLEAN,
        neg_risk_other BOOLEAN,
        cyom BOOLEAN,
        has_reviewed_dates BOOLEAN,
        accepting_orders BOOLEAN,
        accepting_orders_timestamp TEXT,
        automatically_active BOOLEAN,
        clear_book_on_start BOOLEAN,
        manual_activation BOOLEAN,
        pending_deployment BOOLEAN,
        deploying BOOLEAN,
        rfq_enabled BOOLEAN,
        holding_rewards_enabled BOOLEAN,
        fees_enabled BOOLEAN,
        pager_duty_notification_enabled BOOLEAN,
        approved BOOLEAN,
        rewards_min_size REAL,
        rewards_max_spread REAL,
        uma_bond TEXT,
        uma_reward TEXT,
        uma_resolution_statuses TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (event_id) REFERENCES events(id)
    )""")
    
    # ============= TAGS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY,
        label TEXT,
        slug TEXT UNIQUE,
        force_show BOOLEAN,
        force_hide BOOLEAN,
        is_carousel BOOLEAN,
        published_at TEXT,
        created_by INTEGER,
        updated_by INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= MARKET-TAG RELATIONSHIP =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_tags (
        market_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (market_id, tag_id),
        FOREIGN KEY (market_id) REFERENCES markets(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")
    
    # ============= TAG RELATIONSHIPS =============
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
    
    # ============= COMMENTS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id TEXT PRIMARY KEY,
        body TEXT,
        parent_entity_type TEXT,
        parent_entity_id INTEGER,
        parent_comment_id TEXT,
        user_address TEXT,
        reply_address TEXT,
        report_count INTEGER,
        reaction_count INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        profile_name TEXT,
        profile_pseudonym TEXT,
        profile_bio TEXT,
        profile_is_mod BOOLEAN,
        profile_is_creator BOOLEAN,
        profile_proxy_wallet TEXT,
        profile_image TEXT,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= COMMENT REACTIONS =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS comment_reactions (
        id TEXT PRIMARY KEY,
        comment_id TEXT,
        reaction_type TEXT,
        icon TEXT,
        user_address TEXT,
        created_at TIMESTAMP,
        profile_name TEXT,
        profile_proxy_wallet TEXT,
        FOREIGN KEY (comment_id) REFERENCES comments(id)
    )""")
    
    # ============= USER ACTIVITY =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        timestamp INTEGER,
        condition_id TEXT,
        transaction_hash TEXT UNIQUE,
        type TEXT,
        side TEXT,
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
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= SERIES TABLE =============
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
        active BOOLEAN,
        closed BOOLEAN,
        archived BOOLEAN,
        new BOOLEAN,
        featured BOOLEAN,
        restricted BOOLEAN,
        is_template BOOLEAN,
        template_variables BOOLEAN,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        comments_enabled BOOLEAN,
        competitive TEXT,
        volume_24hr REAL,
        volume REAL,
        liquidity REAL,
        start_date TIMESTAMP,
        pyth_token_id TEXT,
        cg_asset_name TEXT,
        score INTEGER,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= SERIES-EVENTS RELATIONSHIP =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_events (
        series_id TEXT,
        event_id TEXT,
        PRIMARY KEY (series_id, event_id),
        FOREIGN KEY (series_id) REFERENCES series(id),
        FOREIGN KEY (event_id) REFERENCES events(id)
    )""")
    
    # ============= COLLECTIONS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collections (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT,
        title TEXT,
        subtitle TEXT,
        collection_type TEXT,
        description TEXT,
        tags TEXT,
        image TEXT,
        icon TEXT,
        header_image TEXT,
        layout TEXT,
        active BOOLEAN,
        closed BOOLEAN,
        archived BOOLEAN,
        new BOOLEAN,
        featured BOOLEAN,
        restricted BOOLEAN,
        is_template BOOLEAN,
        template_variables TEXT,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        comments_enabled BOOLEAN,
        comment_count INTEGER,
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= SERIES-COLLECTIONS RELATIONSHIP =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_collections (
        series_id TEXT,
        collection_id TEXT,
        PRIMARY KEY (series_id, collection_id),
        FOREIGN KEY (series_id) REFERENCES series(id),
        FOREIGN KEY (collection_id) REFERENCES collections(id)
    )""")
    
    # ============= SERIES-TAGS RELATIONSHIP =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS series_tags (
        series_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (series_id, tag_id),
        FOREIGN KEY (series_id) REFERENCES series(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")
    
    # ============= COLLECTION-TAGS RELATIONSHIP =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS collection_tags (
        collection_id TEXT,
        tag_id INTEGER,
        PRIMARY KEY (collection_id, tag_id),
        FOREIGN KEY (collection_id) REFERENCES collections(id),
        FOREIGN KEY (tag_id) REFERENCES tags(id)
    )""")
    
    # ============= USERS TABLE =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        proxy_wallet TEXT PRIMARY KEY,
        username TEXT,
        custom_alias TEXT,
        is_starred BOOLEAN DEFAULT 0,
        pseudonym TEXT,
        bio TEXT,
        profile_image TEXT,
        profile_image_optimized TEXT,
        base_address TEXT,
        is_mod BOOLEAN,
        is_creator BOOLEAN,
        display_username_public BOOLEAN,
        total_value REAL,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    
    # ============= USER POSITIONS CURRENT =============
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
        redeemable BOOLEAN,
        mergeable BOOLEAN,
        negative_risk BOOLEAN,
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
        fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        UNIQUE(proxy_wallet, asset)
    )""")
    
    # ============= USER POSITIONS CLOSED =============
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
        end_date TEXT,
        closed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")
    
    # ============= USER TRADES =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        side TEXT,
        asset TEXT,
        condition_id TEXT,
        size REAL,
        price REAL,
        timestamp INTEGER,
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")
    
    # ============= USER VALUES =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proxy_wallet TEXT,
        market_condition_id TEXT,
        value REAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet)
    )""")
    
    # ============= MARKET HOLDERS =============
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS market_holders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_id TEXT,
        condition_id TEXT,
        token_id TEXT,
        proxy_wallet TEXT,
        username TEXT,
        pseudonym TEXT,
        amount REAL,
        outcome_index INTEGER,
        bio TEXT,
        profile_image TEXT,
        profile_image_optimized TEXT,
        display_username_public BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proxy_wallet) REFERENCES users(proxy_wallet),
        FOREIGN KEY (market_id) REFERENCES markets(id)
    )""")
    
    # ============= CREATE INDEXES =============
    
    # Events indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_slug ON events(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_active ON events(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC)")
    
    # Markets indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_slug ON markets(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_active ON markets(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume DESC)")
    
    # Tags indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_tags_market ON market_tags(market_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_tags_tag ON market_tags(tag_id)")
    
    # Comments indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_entity ON comments(parent_entity_type, parent_entity_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_comments_user ON comments(user_address)")
    
    # User activity indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_wallet ON user_activity(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_activity_timestamp ON user_activity(timestamp DESC)")
    
    # Series indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_slug ON series(slug)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_active ON series(active)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_series_volume ON series(volume DESC)")
    
    # User indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_wallet ON users(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_starred ON users(is_starred)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_wallet ON user_positions_current(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_positions_asset ON user_positions_current(asset)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closed_wallet ON user_positions_closed(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_closed_pnl ON user_positions_closed(realized_pnl DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_wallet ON user_trades(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON user_trades(timestamp DESC)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_trades_hash ON user_trades(transaction_hash)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_values_wallet ON user_values(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_holders_wallet ON market_holders(proxy_wallet)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_holders_market ON market_holders(market_id)")
    
    conn.commit()
    conn.close()
    
    print("✅ Complete database schema created successfully!")
    print("📊 All tables and indexes have been created")
    return True

if __name__ == "__main__":
    create_complete_schema()