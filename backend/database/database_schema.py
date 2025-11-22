"""
Database Schema for Polymarket Terminal
Complete schema definition for all tables including comprehensive market data
"""

def get_schema():
    """Get the complete database schema SQL with all market-related tables"""
    
    # Core tables
    core_tables = """
    -- Events table
    CREATE TABLE IF NOT EXISTS events (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        subtitle TEXT,
        description TEXT,
        resolution_source TEXT,
        start_date TEXT,
        creation_date TEXT,
        end_date TEXT,
        image TEXT,
        icon TEXT,
        featured_image TEXT,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        is_template INTEGER DEFAULT 0,
        template_variables TEXT,
        liquidity REAL DEFAULT 0,
        liquidity_amm REAL DEFAULT 0,
        liquidity_clob REAL DEFAULT 0,
        volume REAL DEFAULT 0,
        volume_clob REAL DEFAULT 0,
        volume_24hr REAL DEFAULT 0,
        volume_24hr_clob REAL DEFAULT 0,
        volume_1wk REAL DEFAULT 0,
        volume_1wk_clob REAL DEFAULT 0,
        volume_1mo REAL DEFAULT 0,
        volume_1mo_clob REAL DEFAULT 0,
        volume_1yr REAL DEFAULT 0,
        volume_1yr_clob REAL DEFAULT 0,
        open_interest REAL DEFAULT 0,
        competitive REAL DEFAULT 0,
        comment_count INTEGER DEFAULT 0,
        tweet_count INTEGER DEFAULT 0,
        enable_order_book INTEGER DEFAULT 0,
        cyom INTEGER DEFAULT 0,
        show_all_outcomes INTEGER DEFAULT 0,
        show_market_images INTEGER DEFAULT 0,
        enable_neg_risk INTEGER DEFAULT 0,
        neg_risk_market_id TEXT,
        neg_risk_fee_bips INTEGER,
        automatically_resolved INTEGER DEFAULT 0,
        automatically_active INTEGER DEFAULT 0,
        closed_time TEXT,
        event_date TEXT,
        start_time TEXT,
        event_week INTEGER,
        series_slug TEXT,
        score TEXT,
        elapsed TEXT,
        period TEXT,
        live INTEGER DEFAULT 0,
        ended INTEGER DEFAULT 0,
        finished_timestamp TEXT,
        gmp_chart_mode TEXT,
        estimate_value INTEGER DEFAULT 0,
        cant_estimate INTEGER DEFAULT 0,
        estimated_value TEXT,
        carousel_map TEXT,
        pending_deployment INTEGER DEFAULT 0,
        deploying INTEGER DEFAULT 0,
        deploying_timestamp TEXT,
        scheduled_deployment_timestamp TEXT,
        game_status TEXT,
        spreads_main_line REAL,
        totals_main_line REAL,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TEXT,
        updated_at TEXT,
        fetched_at TEXT
    );

    -- Markets table (comprehensive)
    CREATE TABLE IF NOT EXISTS markets (
        id TEXT PRIMARY KEY,
        event_id TEXT,
        question TEXT,
        condition_id TEXT UNIQUE,
        slug TEXT,
        twitter_card_image TEXT,
        resolution_source TEXT,
        end_date TEXT,
        start_date TEXT,
        category TEXT,
        subcategory TEXT,
        amm_type TEXT,
        liquidity TEXT,
        liquidity_num REAL DEFAULT 0,
        liquidity_amm REAL DEFAULT 0,
        liquidity_clob REAL DEFAULT 0,
        sponsor_name TEXT,
        sponsor_image TEXT,
        x_axis_value TEXT,
        y_axis_value TEXT,
        denomination_token TEXT,
        fee TEXT,
        image TEXT,
        icon TEXT,
        lower_bound TEXT,
        upper_bound TEXT,
        lower_bound_date TEXT,
        upper_bound_date TEXT,
        description TEXT,
        outcomes TEXT,
        outcome_prices TEXT,
        short_outcomes TEXT,
        volume TEXT,
        volume_num REAL DEFAULT 0,
        volume_amm REAL DEFAULT 0,
        volume_clob REAL DEFAULT 0,
        volume_24hr REAL DEFAULT 0,
        volume_24hr_amm REAL DEFAULT 0,
        volume_24hr_clob REAL DEFAULT 0,
        volume_1wk REAL DEFAULT 0,
        volume_1wk_amm REAL DEFAULT 0,
        volume_1wk_clob REAL DEFAULT 0,
        volume_1mo REAL DEFAULT 0,
        volume_1mo_amm REAL DEFAULT 0,
        volume_1mo_clob REAL DEFAULT 0,
        volume_1yr REAL DEFAULT 0,
        volume_1yr_amm REAL DEFAULT 0,
        volume_1yr_clob REAL DEFAULT 0,
        active INTEGER DEFAULT 1,
        closed INTEGER DEFAULT 0,
        archived INTEGER DEFAULT 0,
        new INTEGER DEFAULT 0,
        featured INTEGER DEFAULT 0,
        restricted INTEGER DEFAULT 0,
        market_type TEXT,
        format_type TEXT,
        market_maker_address TEXT,
        created_by INTEGER,
        updated_by INTEGER,
        created_at TEXT,
        updated_at TEXT,
        closed_time TEXT,
        wide_format INTEGER DEFAULT 0,
        mailchimp_tag TEXT,
        resolved_by TEXT,
        market_group INTEGER,
        group_item_title TEXT,
        group_item_threshold TEXT,
        group_item_range TEXT,
        question_id TEXT,
        uma_end_date TEXT,
        uma_end_date_iso TEXT,
        enable_order_book INTEGER DEFAULT 0,
        order_price_min_tick_size REAL,
        order_min_size REAL,
        uma_resolution_status TEXT,
        uma_resolution_statuses TEXT,
        curation_order INTEGER,
        end_date_iso TEXT,
        start_date_iso TEXT,
        has_reviewed_dates INTEGER DEFAULT 0,
        ready_for_cron INTEGER DEFAULT 0,
        comments_enabled INTEGER DEFAULT 0,
        game_start_time TEXT,
        seconds_delay INTEGER,
        clob_token_ids TEXT,
        disqus_thread TEXT,
        team_a_id TEXT,
        team_b_id TEXT,
        uma_bond TEXT,
        uma_reward TEXT,
        fpmmLive INTEGER DEFAULT 0,
        maker_base_fee REAL,
        taker_base_fee REAL,
        custom_liveness INTEGER,
        accepting_orders INTEGER DEFAULT 0,
        accepting_orders_timestamp TEXT,
        notifications_enabled INTEGER DEFAULT 0,
        score REAL,
        creator TEXT,
        ready INTEGER DEFAULT 0,
        funded INTEGER DEFAULT 0,
        past_slugs TEXT,
        ready_timestamp TEXT,
        funded_timestamp TEXT,
        competitive REAL,
        rewards_min_size REAL,
        rewards_max_spread REAL,
        spread REAL,
        automatically_resolved INTEGER DEFAULT 0,
        one_day_price_change REAL,
        one_hour_price_change REAL,
        one_week_price_change REAL,
        one_month_price_change REAL,
        one_year_price_change REAL,
        last_trade_price REAL,
        best_bid REAL,
        best_ask REAL,
        automatically_active INTEGER DEFAULT 0,
        clear_book_on_start INTEGER DEFAULT 0,
        chart_color TEXT,
        series_color TEXT,
        show_gmp_series INTEGER DEFAULT 0,
        show_gmp_outcome INTEGER DEFAULT 0,
        manual_activation INTEGER DEFAULT 0,
        neg_risk INTEGER DEFAULT 0,
        neg_risk_other INTEGER DEFAULT 0,
        game_id TEXT,
        sports_market_type TEXT,
        line REAL,
        pending_deployment INTEGER DEFAULT 0,
        deploying INTEGER DEFAULT 0,
        deploying_timestamp TEXT,
        scheduled_deployment_timestamp TEXT,
        rfq_enabled INTEGER DEFAULT 0,
        event_start_time TEXT,
        fetched_at TEXT,
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
    );

    -- Image optimization tables
    CREATE TABLE IF NOT EXISTS image_optimized (
        id TEXT PRIMARY KEY,
        image_url_source TEXT,
        image_url_optimized TEXT,
        image_size_kb_source REAL,
        image_size_kb_optimized REAL,
        image_optimized_complete INTEGER DEFAULT 0,
        image_optimized_last_updated TEXT,
        rel_id INTEGER,
        field TEXT,
        relname TEXT,
        entity_type TEXT, -- 'event', 'market', 'series', 'collection'
        entity_id TEXT
    );

    -- Categories table
    CREATE TABLE IF NOT EXISTS categories (
        id TEXT PRIMARY KEY,
        label TEXT,
        parent_category TEXT,
        slug TEXT UNIQUE,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TEXT,
        updated_at TEXT
    );

    -- Collections table
    CREATE TABLE IF NOT EXISTS collections (
        id TEXT PRIMARY KEY,
        ticker TEXT,
        slug TEXT UNIQUE,
        title TEXT,
        subtitle TEXT,
        collection_type TEXT,
        description TEXT,
        tags TEXT,
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
        template_variables TEXT,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TEXT,
        updated_at TEXT,
        comments_enabled INTEGER DEFAULT 0
    );

    -- Series table
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
        template_variables INTEGER DEFAULT 0,
        published_at TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TEXT,
        updated_at TEXT,
        comments_enabled INTEGER DEFAULT 0,
        competitive TEXT,
        volume_24hr REAL DEFAULT 0,
        volume REAL DEFAULT 0,
        liquidity REAL DEFAULT 0,
        start_date TEXT,
        pyth_token_id TEXT,
        cg_asset_name TEXT,
        score REAL,
        fetched_at TEXT
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
        created_by INTEGER,
        updated_by INTEGER,
        created_at TEXT,
        updated_at TEXT,
        fetched_at TEXT
    );

    -- Event creators table
    CREATE TABLE IF NOT EXISTS event_creators (
        id TEXT PRIMARY KEY,
        creator_name TEXT,
        creator_handle TEXT,
        creator_url TEXT,
        creator_image TEXT,
        created_at TEXT,
        updated_at TEXT
    );

    -- Chats table
    CREATE TABLE IF NOT EXISTS chats (
        id TEXT PRIMARY KEY,
        channel_id TEXT,
        channel_name TEXT,
        channel_image TEXT,
        live INTEGER DEFAULT 0,
        start_time TEXT,
        end_time TEXT
    );

    -- Templates table
    CREATE TABLE IF NOT EXISTS templates (
        id TEXT PRIMARY KEY,
        event_title TEXT,
        event_slug TEXT,
        event_image TEXT,
        market_title TEXT,
        description TEXT,
        resolution_source TEXT,
        neg_risk INTEGER DEFAULT 0,
        sort_by TEXT,
        show_market_images INTEGER DEFAULT 0,
        series_slug TEXT,
        outcomes TEXT
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
    """
    
    # Tracking tables
    tracking_tables = """
    -- Live volume tracking table
    CREATE TABLE IF NOT EXISTS event_live_volume (
        event_id TEXT,
        timestamp TEXT,
        volume REAL,
        volume_24hr REAL,
        liquidity REAL,
        PRIMARY KEY (event_id, timestamp),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
    );

    -- Open interest tracking table
    CREATE TABLE IF NOT EXISTS market_open_interest (
        market_id TEXT,
        condition_id TEXT,
        timestamp TEXT,
        open_interest REAL,
        PRIMARY KEY (market_id, timestamp),
        FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE
    );

    -- Market holders table
    CREATE TABLE IF NOT EXISTS market_holders (
        market_id TEXT,
        proxy_wallet TEXT,
        shares REAL DEFAULT 0,
        avg_price REAL DEFAULT 0,
        PRIMARY KEY (market_id, proxy_wallet),
        FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE
    );
    """
    
    # Relationship tables
    relationship_tables = """
    -- Event tags relationship table
    CREATE TABLE IF NOT EXISTS event_tags (
        event_id TEXT,
        tag_id TEXT,
        tag_slug TEXT,
        PRIMARY KEY (event_id, tag_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
        -- Note: FK on tag_id removed - tags may not exist when events are loaded
    );

    -- Market tags relationship table
    CREATE TABLE IF NOT EXISTS market_tags (
        market_id TEXT,
        tag_id TEXT,
        tag_slug TEXT,
        PRIMARY KEY (market_id, tag_id)
        -- Note: FK constraints removed - data can be loaded in any order
    );

    -- Market categories relationship table
    CREATE TABLE IF NOT EXISTS market_categories (
        market_id TEXT,
        category_id TEXT,
        PRIMARY KEY (market_id, category_id),
        FOREIGN KEY (market_id) REFERENCES markets(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

    -- Event series relationship table
    CREATE TABLE IF NOT EXISTS event_series (
        event_id TEXT,
        series_id TEXT,
        PRIMARY KEY (event_id, series_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    );

    -- Event collections relationship table
    CREATE TABLE IF NOT EXISTS event_collections (
        event_id TEXT,
        collection_id TEXT,
        PRIMARY KEY (event_id, collection_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
    );

    -- Event categories relationship table
    CREATE TABLE IF NOT EXISTS event_categories (
        event_id TEXT,
        category_id TEXT,
        PRIMARY KEY (event_id, category_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

    -- Event creators relationship table
    CREATE TABLE IF NOT EXISTS event_event_creators (
        event_id TEXT,
        creator_id TEXT,
        PRIMARY KEY (event_id, creator_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (creator_id) REFERENCES event_creators(id) ON DELETE CASCADE
    );

    -- Event chats relationship table
    CREATE TABLE IF NOT EXISTS event_chats (
        event_id TEXT,
        chat_id TEXT,
        PRIMARY KEY (event_id, chat_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
    );

    -- Event templates relationship table
    CREATE TABLE IF NOT EXISTS event_templates (
        event_id TEXT,
        template_id TEXT,
        PRIMARY KEY (event_id, template_id),
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE,
        FOREIGN KEY (template_id) REFERENCES templates(id) ON DELETE CASCADE
    );

    -- Series tags relationship table (JSON storage)
    CREATE TABLE IF NOT EXISTS series_tags (
        series_id TEXT PRIMARY KEY,
        tag_ids TEXT,  -- JSON array of tag IDs
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    );

    -- Series categories relationship table (JSON storage)
    CREATE TABLE IF NOT EXISTS series_categories (
        series_id TEXT PRIMARY KEY,
        category_ids TEXT,  -- JSON array of category IDs
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    );

    -- Series collections relationship table (JSON storage)
    CREATE TABLE IF NOT EXISTS series_collections (
        series_id TEXT PRIMARY KEY,
        collection_ids TEXT,  -- JSON array of collection IDs
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    );

    -- Series chats relationship table (JSON storage)
    CREATE TABLE IF NOT EXISTS series_chats (
        series_id TEXT PRIMARY KEY,
        chat_ids TEXT,  -- JSON array of chat IDs
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    );

    -- Series events relationship table (JSON storage)
    CREATE TABLE IF NOT EXISTS series_events (
        series_id TEXT PRIMARY KEY,
        event_ids TEXT,  -- JSON array of event IDs
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
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
    """
    
    # User activity tables
    user_tables = """
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
        PRIMARY KEY (proxy_wallet, market_condition_id),
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
    """
    
    # Indexes for performance
    indexes = """
    -- Create indexes for better query performance
    CREATE INDEX IF NOT EXISTS idx_events_closed ON events(closed);
    CREATE INDEX IF NOT EXISTS idx_events_volume ON events(volume DESC);
    CREATE INDEX IF NOT EXISTS idx_events_updated ON events(updated_at DESC);

    CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);
    CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
    CREATE INDEX IF NOT EXISTS idx_markets_volume ON markets(volume_num DESC);
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
    """
    
    # Database pragmas
    pragmas = """
    -- Enable foreign key constraints
    PRAGMA foreign_keys = ON;

    -- Set optimal pragmas for performance
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA cache_size = -64000;
    PRAGMA temp_store = MEMORY;
    """
    
    # Combine all schema parts
    full_schema = (
        core_tables + 
        tracking_tables + 
        relationship_tables +
        user_tables + 
        indexes + 
        pragmas
    )
    
    return full_schema

# For backward compatibility
SCHEMA = get_schema()