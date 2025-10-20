from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime

class MarketInfo(BaseModel):
    """Market information schema"""
    id: str
    question: str
    market_slug: Optional[str] = None
    volume: Optional[float] = 0
    liquidity: Optional[float] = 0

class TradeData(BaseModel):
    """Individual trade data"""
    market_id: str
    side: str  # BUY or SELL
    price: float
    size: float
    value: float
    timestamp: str

class BetData(BaseModel):
    """Bet information schema"""
    id: Optional[str] = None
    market: MarketInfo
    maker: str
    taker: str
    side: str
    price: float
    size: float
    value: float
    timestamp: str
    is_whale: bool = False

class WalletStats(BaseModel):
    """Wallet statistics schema"""
    address: str
    total_volume: float
    total_bets: int
    win_rate: float
    profit_loss: float
    largest_bet: float
    markets_traded: int
    last_activity: Optional[str] = None

class WhaleData(BaseModel):
    """Complete whale data schema"""
    address: str
    total_volume: float
    total_bets: int
    winning_bets: int
    losing_bets: int
    pending_bets: int
    profit_loss: float
    largest_bet: float
    recent_trades: List[TradeData]
    markets_traded: List[str]
    last_activity: Optional[str] = None
    win_rate: float

class MarketActivity(BaseModel):
    """Market activity with whale presence"""
    market_id: str
    question: str
    whale_count: int
    total_whale_volume: float
    recent_trades: List[Dict[str, Any]]

class SummaryStats(BaseModel):
    """Overall summary statistics"""
    total_whales_tracked: int
    total_volume_tracked: float
    total_bets_tracked: int
    average_win_rate: float
    last_update: Optional[str] = None
    recent_large_bets_count: int