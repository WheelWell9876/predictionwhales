from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from datetime import datetime
import uvicorn

from services.polymarket_service import PolymarketService
from services.whale_tracker import WhaleTracker
from models.schemas import WhaleData, BetData, WalletStats
from config import settings

# Background task for continuous tracking
background_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global background_task
    background_task = asyncio.create_task(continuous_tracking())
    yield
    # Shutdown
    if background_task:
        background_task.cancel()
        try:
            await background_task
        except asyncio.CancelledError:
            pass

app = FastAPI(title="Polymarket Whale Tracker", lifespan=lifespan)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
polymarket_service = PolymarketService()
whale_tracker = WhaleTracker(polymarket_service)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

async def continuous_tracking():
    """Background task to continuously track whale activity"""
    while True:
        try:
            await whale_tracker.update_whale_data()
            await asyncio.sleep(settings.UPDATE_INTERVAL)  # Update every 5 minutes
        except Exception as e:
            print(f"Error in continuous tracking: {e}")
            await asyncio.sleep(60)  # Wait 1 minute on error

@app.get("/")
async def read_root():
    """Serve the main HTML page"""
    return FileResponse('static/index.html')

@app.get("/api/whales")
async def get_whales():
    """Get all tracked whale wallets with their stats"""
    try:
        whales = await whale_tracker.get_whale_stats()
        return {"success": True, "data": whales}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/whales/{wallet_address}")
async def get_whale_details(wallet_address: str):
    """Get detailed information about a specific whale wallet"""
    try:
        details = await whale_tracker.get_wallet_details(wallet_address)
        if not details:
            raise HTTPException(status_code=404, detail="Wallet not found")
        return {"success": True, "data": details}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/bets/recent")
async def get_recent_large_bets(min_amount: float = 1000):
    """Get recent large bets across all markets"""
    try:
        bets = await whale_tracker.get_recent_large_bets(min_amount)
        return {"success": True, "data": bets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/markets/active")
async def get_active_markets():
    """Get active markets with significant whale activity"""
    try:
        markets = await whale_tracker.get_active_markets_with_whale_activity()
        return {"success": True, "data": markets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/track/wallet/{wallet_address}")
async def track_wallet(wallet_address: str, background_tasks: BackgroundTasks):
    """Manually add a wallet to track"""
    try:
        success = await whale_tracker.add_wallet_to_track(wallet_address)
        if success:
            background_tasks.add_task(whale_tracker.fetch_wallet_history, wallet_address)
            return {"success": True, "message": f"Now tracking wallet {wallet_address}"}
        else:
            return {"success": False, "message": "Wallet already being tracked"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats/summary")
async def get_summary_stats():
    """Get summary statistics of all whale activity"""
    try:
        stats = await whale_tracker.get_summary_stats()
        return {"success": True, "data": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/refresh")
async def force_refresh():
    """Force a refresh of whale data"""
    try:
        await whale_tracker.update_whale_data()
        return {"success": True, "message": "Data refresh initiated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)