# """
# Google Colab Runner for Polymarket Whale Tracker
# Run this entire script in a Colab cell
# """
#
# # ============================================
# # PART 1: Setup Environment
# # ============================================
# print("🚀 Setting up Polymarket Whale Tracker in Colab...")
# print("=" * 60)
#
# # Install dependencies
# print("📦 Installing dependencies...")
# !pip
# install
# fastapi
# uvicorn
# aiohttp
# web3
# eth - account
# python - dotenv
# pydantic
# pydantic - settings
# nest - asyncio
# pyngrok - q
#
# # Clone repository
# print("📥 Cloning repository...")
# !git
# clone
# https: // github.com / WheelWell9876 / predictionwhales.git
# 2 > / dev / null | | echo
# "Repo already cloned"
# %cd
# predictionwhales
#
# # Create directory structure
# import os
#
# os.makedirs('services', exist_ok=True)
# os.makedirs('models', exist_ok=True)
# os.makedirs('static/css', exist_ok=True)
# os.makedirs('static/js', exist_ok=True)
#
# # Create __init__ files
# !touch
# services / __init__.py
# models / __init__.py
#
# print("✅ Environment setup complete!")
#
# # ============================================
# # PART 2: Create Configuration
# # ============================================
# print("\n📝 Creating configuration...")
#
# # Create .env file
# env_content = """# Polymarket Configuration
# OBSERVER_PRIVATE_KEY=0x0000000000000000000000000000000000000000000000000000000000000001
# OBSERVER_ADDRESS=0x0000000000000000000000000000000000000000
# POLYGON_RPC_URL=https://polygon-rpc.com
# MIN_BET_AMOUNT=1000
# MIN_WHALE_VOLUME=10000
# UPDATE_INTERVAL=300
# MAX_TRACKED_WALLETS=100
# MAX_CONCURRENT_REQUESTS=10
# """
#
# with open('.env', 'w') as f:
#     f.write(env_content)
#
# # Fix config.py for Colab compatibility
# config_py = """
# from pydantic_settings import BaseSettings
# from typing import Optional
# import os
#
# class Settings(BaseSettings):
#     POLYMARKET_API_KEY: Optional[str] = None
#     MIN_BET_AMOUNT: float = 1000
#     MIN_WHALE_VOLUME: float = 10000
#     UPDATE_INTERVAL: int = 300
#     DATABASE_URL: Optional[str] = "sqlite:///./whale_tracker.db"
#     MAX_CONCURRENT_REQUESTS: int = 10
#     REQUEST_TIMEOUT: int = 30
#     MAX_TRACKED_WALLETS: int = 100
#     MAX_RECENT_BETS: int = 500
#
#     class Config:
#         env_file = ".env"
#         case_sensitive = True
#
# settings = Settings()
# """
#
# with open('config.py', 'w') as f:
#     f.write(config_py)
#
# print("✅ Configuration created!")
#
# # ============================================
# # PART 3: Test Polymarket APIs
# # ============================================
# print("\n🔍 Testing Polymarket API connections...")
# print("-" * 60)
#
# import asyncio
# import aiohttp
# import nest_asyncio
#
# nest_asyncio.apply()
#
#
# async def test_apis():
#     apis = {
#         'CLOB': 'https://clob.polymarket.com/trades?limit=2',
#         'Gamma': 'https://gamma-api.polymarket.com/markets?limit=2',
#     }
#
#     async with aiohttp.ClientSession() as session:
#         for name, url in apis.items():
#             try:
#                 async with session.get(url, timeout=5) as response:
#                     if response.status == 200:
#                         data = await response.json()
#                         print(f"✅ {name} API: Connected")
#                         if data and isinstance(data, list) and len(data) > 0:
#                             print(f"   Sample: {str(data[0])[:100]}...")
#                     else:
#                         print(f"❌ {name} API: Status {response.status}")
#             except Exception as e:
#                 print(f"❌ {name} API: {str(e)[:50]}")
#             print()
#
#
# await test_apis()
#
# # ============================================
# # PART 4: Create Minimal Working App
# # ============================================
# print("\n📱 Creating minimal FastAPI app...")
#
# app_code = """
# from fastapi import FastAPI, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# import aiohttp
# import asyncio
# from typing import List, Dict
# from datetime import datetime
#
# app = FastAPI(title="Polymarket Whale Tracker - Colab")
#
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
#
# # Simple in-memory storage
# whale_data = {
#     "whales": [],
#     "recent_trades": [],
#     "last_update": None
# }
#
# @app.get("/")
# async def root():
#     return {
#         "status": "running",
#         "message": "🐋 Polymarket Whale Tracker Active",
#         "last_update": whale_data["last_update"],
#         "endpoints": [
#             "/api/whales",
#             "/api/trades",
#             "/api/markets",
#             "/api/refresh"
#         ]
#     }
#
# @app.get("/api/whales")
# async def get_whales():
#     \"\"\"Get whale wallets\"\"\"
#     return {
#         "success": True,
#         "data": whale_data["whales"],
#         "count": len(whale_data["whales"])
#     }
#
# @app.get("/api/trades")
# async def get_trades():
#     \"\"\"Get recent large trades\"\"\"
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.get("https://clob.polymarket.com/trades?limit=50") as response:
#                 if response.status == 200:
#                     trades = await response.json()
#
#                     # Filter large trades
#                     large_trades = []
#                     for trade in trades[:20]:
#                         try:
#                             size = float(trade.get('size', 0))
#                             price = float(trade.get('price', 0))
#                             value = size * price / 1e6
#
#                             if value >= 1000:  # $1000+ trades
#                                 trade['usd_value'] = value
#                                 large_trades.append(trade)
#                         except:
#                             continue
#
#                     whale_data["recent_trades"] = large_trades
#                     whale_data["last_update"] = datetime.now().isoformat()
#
#                     return {
#                         "success": True,
#                         "data": large_trades,
#                         "count": len(large_trades)
#                     }
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
#
#     return {"success": False, "data": []}
#
# @app.get("/api/markets")
# async def get_markets():
#     \"\"\"Get active markets\"\"\"
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.get("https://gamma-api.polymarket.com/markets?active=true&limit=10") as response:
#                 if response.status == 200:
#                     markets = await response.json()
#                     return {
#                         "success": True,
#                         "data": markets,
#                         "count": len(markets)
#                     }
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
#
#     return {"success": False, "data": []}
#
# @app.post("/api/refresh")
# async def refresh_data():
#     \"\"\"Force refresh of whale data\"\"\"
#     # Fetch fresh data
#     await get_trades()
#     return {
#         "success": True,
#         "message": "Data refreshed",
#         "timestamp": datetime.now().isoformat()
#     }
#
# @app.get("/api/stats")
# async def get_stats():
#     \"\"\"Get summary statistics\"\"\"
#     return {
#         "success": True,
#         "data": {
#             "total_whales": len(whale_data["whales"]),
#             "recent_trades": len(whale_data["recent_trades"]),
#             "last_update": whale_data["last_update"]
#         }
#     }
# """
#
# with open('colab_app.py', 'w') as f:
#     f.write(app_code)
#
# print("✅ App created!")
#
# # ============================================
# # PART 5: Run the Server
# # ============================================
# print("\n🚀 Starting FastAPI server with ngrok...")
#
# from pyngrok import ngrok
# import uvicorn
# from threading import Thread
# import time
#
# # Kill any existing ngrok connections
# ngrok.kill()
#
#
# # Start FastAPI in background thread
# def run_server():
#     uvicorn.run("colab_app:app", host="0.0.0.0", port=8000)
#
#
# thread = Thread(target=run_server)
# thread.daemon = True
# thread.start()
#
# # Wait for server to start
# time.sleep(5)
#
# # Create ngrok tunnel
# try:
#     public_url = ngrok.connect(8000).public_url
#
#     print("\n" + "=" * 60)
#     print("🎉 SUCCESS! Polymarket Whale Tracker is running!")
#     print("=" * 60)
#     print(f"\n📡 Public URL: {public_url}")
#     print(f"📊 API Documentation: {public_url}/docs")
#     print(f"🔍 Test Endpoints:")
#     print(f"   - Root: {public_url}/")
#     print(f"   - Trades: {public_url}/api/trades")
#     print(f"   - Markets: {public_url}/api/markets")
#     print(f"   - Stats: {public_url}/api/stats")
#     print("\n⚠️  This URL will work for 2 hours (ngrok limit)")
#     print("=" * 60)
#
#     # Test the API
#     import requests
#
#     response = requests.get(f"{public_url}/api/stats")
#     print(f"\n✅ API Test: {response.json()}")
#
# except Exception as e:
#     print(f"❌ Error starting ngrok: {e}")
#     print("Try running: !ngrok authtoken YOUR_TOKEN")
#
# # ============================================
# # PART 6: Create Simple Web Interface
# # ============================================
# print("\n📱 Creating web interface...")
#
# html_content = f"""
# <!DOCTYPE html>
# <html>
# <head>
#     <title>🐋 Polymarket Whale Tracker</title>
#     <style>
#         body {{
#             font-family: Arial, sans-serif;
#             background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
#             color: white;
#             padding: 20px;
#         }}
#         .container {{
#             max-width: 1200px;
#             margin: 0 auto;
#         }}
#         .card {{
#             background: rgba(255,255,255,0.1);
#             border-radius: 10px;
#             padding: 20px;
#             margin: 20px 0;
#         }}
#         button {{
#             background: #4CAF50;
#             color: white;
#             border: none;
#             padding: 10px 20px;
#             border-radius: 5px;
#             cursor: pointer;
#         }}
#         table {{
#             width: 100%;
#             border-collapse: collapse;
#         }}
#         th, td {{
#             padding: 10px;
#             text-align: left;
#             border-bottom: 1px solid rgba(255,255,255,0.2);
#         }}
#     </style>
# </head>
# <body>
#     <div class="container">
#         <h1>🐋 Polymarket Whale Tracker</h1>
#         <div class="card">
#             <h2>API Status</h2>
#             <p>Server: <span id="status">Loading...</span></p>
#             <p>Last Update: <span id="lastUpdate">Never</span></p>
#             <button onclick="refreshData()">Refresh Data</button>
#         </div>
#
#         <div class="card">
#             <h2>Recent Large Trades</h2>
#             <div id="trades">Loading...</div>
#         </div>
#
#         <div class="card">
#             <h2>Active Markets</h2>
#             <div id="markets">Loading...</div>
#         </div>
#     </div>
#
#     <script>
#         const API_URL = '{public_url}';
#
#         async function loadData() {{
#             try {{
#                 // Load status
#                 const statusRes = await fetch(API_URL + '/');
#                 const status = await statusRes.json();
#                 document.getElementById('status').textContent = status.status;
#
#                 // Load trades
#                 const tradesRes = await fetch(API_URL + '/api/trades');
#                 const trades = await tradesRes.json();
#
#                 if (trades.success && trades.data.length > 0) {{
#                     let html = '<table><tr><th>Market</th><th>Value (USD)</th><th>Price</th></tr>';
#                     trades.data.forEach(t => {{
#                         html += '<tr><td>' + (t.market || 'Unknown').substring(0,30) + '...</td>';
#                         html += '<td>$' + (t.usd_value || 0).toFixed(2) + '</td>';
#                         html += '<td>' + (t.price || 0).toFixed(4) + '</td></tr>';
#                     }});
#                     html += '</table>';
#                     document.getElementById('trades').innerHTML = html;
#                 }}
#
#                 // Load markets
#                 const marketsRes = await fetch(API_URL + '/api/markets');
#                 const markets = await marketsRes.json();
#
#                 if (markets.success && markets.data.length > 0) {{
#                     let html = '<table><tr><th>Question</th><th>Volume</th></tr>';
#                     markets.data.forEach(m => {{
#                         html += '<tr><td>' + (m.question || 'Unknown').substring(0,50) + '...</td>';
#                         html += '<td>$' + (m.volume || 0).toFixed(0) + '</td></tr>';
#                     }});
#                     html += '</table>';
#                     document.getElementById('markets').innerHTML = html;
#                 }}
#
#                 document.getElementById('lastUpdate').textContent = new Date().toLocaleTimeString();
#
#             }} catch(e) {{
#                 console.error('Error loading data:', e);
#             }}
#         }}
#
#         async function refreshData() {{
#             await fetch(API_URL + '/api/refresh', {{method: 'POST'}});
#             await loadData();
#         }}
#
#         // Load data on page load
#         loadData();
#
#         // Auto-refresh every 30 seconds
#         setInterval(loadData, 30000);
#     </script>
# </body>
# </html>
# """
#
# # Save HTML file
# with open('tracker.html', 'w') as f:
#     f.write(html_content)
#
# print(f"✅ Web interface created!")
# print(f"\n🌐 View in browser: {public_url}")
# print("\nOr download tracker.html and open locally")
#
# print("\n" + "=" * 60)
# print("📋 SUMMARY")
# print("=" * 60)
# print("✅ Repository cloned")
# print("✅ Dependencies installed")
# print("✅ API connections tested")
# print("✅ FastAPI server running")
# print("✅ Public URL available via ngrok")
# print("\n🎉 Your Polymarket Whale Tracker is live!")
# print("Access it at:", public_url)