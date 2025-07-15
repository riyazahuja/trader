import pandas as pd
import ta


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute indicators on a dataframe with columns ['open','high','low','close','volume']."""
    df = df.copy()
    df["ATR"] = ta.volatility.average_true_range(
        df["high"], df["low"], df["close"], window=14
    )
    df["EMA8"] = ta.trend.ema_indicator(df["close"], window=8)
    df["EMA20"] = ta.trend.ema_indicator(df["close"], window=20)
    df["VWAP"] = ta.volume.volume_weighted_average_price(
        df["high"], df["low"], df["close"], df["volume"]
    )
    df["RSI"] = ta.momentum.rsi(df["close"], window=14)
    df["VOL_AVG5"] = df["volume"].rolling(window=5).mean()
    return df
