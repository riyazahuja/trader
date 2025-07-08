import os

# API keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
FINNHUB_KEY = os.getenv("FINNHUB_KEY")

# Trading parameters
defaults = {
    "atr_window": 14,
    "ema_short": 8,
    "ema_long": 20,
    "rsi_window": 14,
    "risk_pct": 0.01,
    "max_total_risk": 0.05,
    "max_open_trades": 5,
}

WATCHLIST = ["AAPL", "MSFT", "GOOG"]
