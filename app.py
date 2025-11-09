"""
Flask Backend for Polymarket Trading Terminal
Main application server with API endpoints
"""

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta
import os
import sqlite3

app = Flask(__name__, static_folder='../frontend/build', static_url_path='')
CORS(app)

# Database configuration
DATABASE_PATH = os.getenv('DATABASE_PATH', 'polymarket_terminal.db')
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DATABASE_PATH}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Initialize database
db = SQLAlchemy(app)

# ============= HELPER FUNCTIONS =============

def get_db_connection():
    """Create a database connection"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def dict_from_row(row):
    """Convert sqlite3.Row to dictionary"""
    return dict(zip(row.keys(), row))

# ============= USER ENDPOINTS =============

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get all users with their current positions and values"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    sort_by = request.args.get('sort_by', 'total_value')
    sort_order = request.args.get('sort_order', 'DESC')
    
    offset = (page - 1) * limit
    
    # Get users with aggregated position data
    query = f"""
        SELECT 
            u.proxy_wallet,
            u.username,
            u.custom_alias,
            u.is_starred,
            u.pseudonym,
            u.bio,
            u.profile_image,
            u.total_value,
            u.last_updated,
            COUNT(DISTINCT pc.id) as active_positions,
            COALESCE(SUM(pc.current_value), 0) as positions_value,
            COALESCE(SUM(pc.cash_pnl), 0) as total_pnl
        FROM users u
        LEFT JOIN user_positions_current pc ON u.proxy_wallet = pc.proxy_wallet
        GROUP BY u.proxy_wallet
        ORDER BY {sort_by} {sort_order}
        LIMIT ? OFFSET ?
    """
    
    cursor.execute(query, (limit, offset))
    users = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get total count for pagination
    cursor.execute("SELECT COUNT(*) as count FROM users")
    total_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        'users': users,
        'total': total_count,
        'page': page,
        'limit': limit
    })

@app.route('/api/users/<wallet_address>', methods=['GET'])
def get_user_detail(wallet_address):
    """Get detailed information for a specific user"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get user info
    cursor.execute("""
        SELECT * FROM users WHERE proxy_wallet = ?
    """, (wallet_address,))
    user = dict_from_row(cursor.fetchone()) if cursor.fetchone() else None
    
    if not user:
        return jsonify({'error': 'User not found'}), 404
    
    # Get current positions
    cursor.execute("""
        SELECT * FROM user_positions_current 
        WHERE proxy_wallet = ? 
        ORDER BY current_value DESC
    """, (wallet_address,))
    positions = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get recent trades
    cursor.execute("""
        SELECT * FROM user_trades 
        WHERE proxy_wallet = ? 
        ORDER BY timestamp DESC 
        LIMIT 100
    """, (wallet_address,))
    trades = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get portfolio values history
    cursor.execute("""
        SELECT * FROM user_values 
        WHERE proxy_wallet = ? 
        ORDER BY timestamp DESC 
        LIMIT 1000
    """, (wallet_address,))
    values = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        'user': user,
        'positions': positions,
        'trades': trades,
        'values': values
    })

@app.route('/api/users/<wallet_address>/star', methods=['POST'])
def star_user(wallet_address):
    """Star/unstar a user"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    data = request.json
    is_starred = data.get('is_starred', True)
    custom_alias = data.get('custom_alias', None)
    
    cursor.execute("""
        UPDATE users 
        SET is_starred = ?, custom_alias = ?
        WHERE proxy_wallet = ?
    """, (is_starred, custom_alias, wallet_address))
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True})

# ============= EVENTS ENDPOINTS =============

@app.route('/api/events', methods=['GET'])
def get_events():
    """Get all events with filtering and sorting"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    active_only = request.args.get('active_only', 'true').lower() == 'true'
    sort_by = request.args.get('sort_by', 'volume')
    sort_order = request.args.get('sort_order', 'DESC')
    
    offset = (page - 1) * limit
    
    where_clause = "WHERE active = 1" if active_only else ""
    
    query = f"""
        SELECT 
            e.*,
            COUNT(DISTINCT m.id) as market_count
        FROM events e
        LEFT JOIN markets m ON e.id = m.event_id
        {where_clause}
        GROUP BY e.id
        ORDER BY e.{sort_by} {sort_order}
        LIMIT ? OFFSET ?
    """
    
    cursor.execute(query, (limit, offset))
    events = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get total count
    count_query = f"SELECT COUNT(*) as count FROM events {where_clause}"
    cursor.execute(count_query)
    total_count = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify({
        'events': events,
        'total': total_count,
        'page': page,
        'limit': limit
    })

@app.route('/api/events/<event_id>', methods=['GET'])
def get_event_detail(event_id):
    """Get detailed information for a specific event"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get event info
    cursor.execute("SELECT * FROM events WHERE id = ?", (event_id,))
    event = dict_from_row(cursor.fetchone()) if cursor.fetchone() else None
    
    if not event:
        return jsonify({'error': 'Event not found'}), 404
    
    # Get markets for this event
    cursor.execute("""
        SELECT * FROM markets 
        WHERE event_id = ? 
        ORDER BY volume DESC
    """, (event_id,))
    markets = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get tags for this event
    cursor.execute("""
        SELECT t.* FROM tags t
        JOIN market_tags mt ON t.id = mt.tag_id
        JOIN markets m ON mt.market_id = m.id
        WHERE m.event_id = ?
        GROUP BY t.id
    """, (event_id,))
    tags = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get comments
    cursor.execute("""
        SELECT * FROM comments 
        WHERE parent_entity_id = ? AND parent_entity_type = 'event'
        ORDER BY created_at DESC
    """, (event_id,))
    comments = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        'event': event,
        'markets': markets,
        'tags': tags,
        'comments': comments
    })

# ============= MARKETS ENDPOINTS =============

@app.route('/api/markets', methods=['GET'])
def get_markets():
    """Get all markets with filtering"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 50, type=int)
    event_id = request.args.get('event_id', None)
    sort_by = request.args.get('sort_by', 'volume')
    sort_order = request.args.get('sort_order', 'DESC')
    
    offset = (page - 1) * limit
    
    where_conditions = []
    params = []
    
    if event_id:
        where_conditions.append("event_id = ?")
        params.append(event_id)
    
    where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
    
    query = f"""
        SELECT * FROM markets
        {where_clause}
        ORDER BY {sort_by} {sort_order}
        LIMIT ? OFFSET ?
    """
    
    params.extend([limit, offset])
    cursor.execute(query, params)
    markets = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        'markets': markets,
        'page': page,
        'limit': limit
    })

@app.route('/api/markets/<market_id>', methods=['GET'])
def get_market_detail(market_id):
    """Get detailed information for a specific market"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get market info
    cursor.execute("SELECT * FROM markets WHERE id = ?", (market_id,))
    market = dict_from_row(cursor.fetchone()) if cursor.fetchone() else None
    
    if not market:
        return jsonify({'error': 'Market not found'}), 404
    
    # Get holders
    cursor.execute("""
        SELECT * FROM market_holders 
        WHERE market_id = ? 
        ORDER BY amount DESC
        LIMIT 100
    """, (market_id,))
    holders = [dict_from_row(row) for row in cursor.fetchall()]
    
    # Get recent trades
    cursor.execute("""
        SELECT * FROM user_trades 
        WHERE condition_id = ? 
        ORDER BY timestamp DESC 
        LIMIT 100
    """, (market['condition_id'],))
    trades = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({
        'market': market,
        'holders': holders,
        'trades': trades
    })

# ============= WHALES ENDPOINTS =============

@app.route('/api/whales', methods=['GET'])
def get_whales():
    """Get top whale traders"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get whales based on total value and trading volume
    cursor.execute("""
        SELECT 
            u.*,
            COUNT(DISTINCT ut.id) as total_trades,
            COALESCE(SUM(ut.size * ut.price), 0) as total_volume,
            COUNT(DISTINCT pc.id) as active_positions,
            COALESCE(SUM(pc.current_value), 0) as positions_value,
            COALESCE(SUM(pc.cash_pnl), 0) as total_pnl
        FROM users u
        LEFT JOIN user_trades ut ON u.proxy_wallet = ut.proxy_wallet
        LEFT JOIN user_positions_current pc ON u.proxy_wallet = pc.proxy_wallet
        GROUP BY u.proxy_wallet
        HAVING total_volume > 10000 OR positions_value > 10000
        ORDER BY positions_value DESC
        LIMIT 100
    """)
    
    whales = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({'whales': whales})

# ============= SERIES ENDPOINTS =============

@app.route('/api/series', methods=['GET'])
def get_series():
    """Get all series"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            s.*,
            COUNT(DISTINCT se.event_id) as event_count
        FROM series s
        LEFT JOIN series_events se ON s.id = se.series_id
        WHERE s.active = 1
        GROUP BY s.id
        ORDER BY s.volume DESC
        LIMIT 100
    """)
    
    series = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({'series': series})

# ============= TAGS ENDPOINTS =============

@app.route('/api/tags', methods=['GET'])
def get_tags():
    """Get all tags with usage counts"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            t.*,
            COUNT(DISTINCT mt.market_id) as market_count
        FROM tags t
        LEFT JOIN market_tags mt ON t.id = mt.tag_id
        GROUP BY t.id
        ORDER BY market_count DESC
    """)
    
    tags = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({'tags': tags})

# ============= STATS ENDPOINTS =============

@app.route('/api/stats/overview', methods=['GET'])
def get_stats_overview():
    """Get platform overview statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {}
    
    # Get totals
    cursor.execute("SELECT COUNT(*) as count FROM users")
    stats['total_users'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM events WHERE active = 1")
    stats['active_events'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM markets WHERE active = 1")
    stats['active_markets'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT SUM(volume) as total FROM markets")
    stats['total_volume'] = cursor.fetchone()['total'] or 0
    
    cursor.execute("SELECT SUM(liquidity) as total FROM markets")
    stats['total_liquidity'] = cursor.fetchone()['total'] or 0
    
    cursor.execute("SELECT COUNT(*) as count FROM user_trades")
    stats['total_trades'] = cursor.fetchone()['count']
    
    conn.close()
    
    return jsonify(stats)

# ============= CHART DATA ENDPOINTS =============

@app.route('/api/charts/user/<wallet_address>/portfolio', methods=['GET'])
def get_user_portfolio_chart(wallet_address):
    """Get portfolio value history for charting"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    timeframe = request.args.get('timeframe', '7d')
    
    # Calculate date range
    end_date = datetime.now()
    if timeframe == '24h':
        start_date = end_date - timedelta(days=1)
    elif timeframe == '7d':
        start_date = end_date - timedelta(days=7)
    elif timeframe == '30d':
        start_date = end_date - timedelta(days=30)
    elif timeframe == '90d':
        start_date = end_date - timedelta(days=90)
    else:
        start_date = end_date - timedelta(days=365)
    
    cursor.execute("""
        SELECT timestamp, value 
        FROM user_values 
        WHERE proxy_wallet = ? AND timestamp >= ?
        ORDER BY timestamp ASC
    """, (wallet_address, start_date))
    
    data = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({'data': data})

@app.route('/api/charts/market/<market_id>/price', methods=['GET'])
def get_market_price_chart(market_id):
    """Get market price history for charting"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # This would need to be implemented with actual price history data
    # For now, returning mock data structure
    cursor.execute("""
        SELECT 
            timestamp, 
            price as close,
            price as open,
            price as high,
            price as low,
            size as volume
        FROM user_trades 
        WHERE condition_id = (SELECT condition_id FROM markets WHERE id = ?)
        ORDER BY timestamp ASC
        LIMIT 1000
    """, (market_id,))
    
    data = [dict_from_row(row) for row in cursor.fetchall()]
    
    conn.close()
    
    return jsonify({'data': data})

# ============= STATIC FILE SERVING =============

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    """Serve React application"""
    if path != "" and os.path.exists(app.static_folder + '/' + path):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

# ============= ERROR HANDLERS =============

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # Initialize database if needed
    from backend import database_schema

    database_schema.create_complete_schema(DATABASE_PATH)
    
    # Run the application
    app.run(debug=True, host='0.0.0.0', port=5000)