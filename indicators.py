import gc
import logging
from typing import Optional

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator, MACD
from ta.volatility import BollingerBands
from binance.client import Client

logger = logging.getLogger(__name__)

KLINE_INTERVAL = Client.KLINE_INTERVAL_1HOUR
KLINE_LIMIT = 250  # EMA50 için en az 200 mum gerekir, 250 ile warm-up payı bırakılır
PREFILTER_TOP_N = 40

EXCLUDED_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
]

BANNED_PARTS = [
    "UPUSDT",
    "DOWNUSDT",
    "BULLUSDT",
    "BEARUSDT",
    "FDUSD",
    "TUSD",
    "USDC",
]


def safe_float(value) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def is_valid_symbol(symbol: str) -> bool:
    if not symbol.endswith("USDT"):
        return False
    if symbol in EXCLUDED_SYMBOLS:
        return False
    for part in BANNED_PARTS:
        if part in symbol:
            return False
    return True


def prefilter_candidates(tickers_map: dict, min_volume: float) -> list[str]:
    """
    Birinci geçiş: ticker verisiyle hızlı filtre.
    Hacme göre sıralı en fazla PREFILTER_TOP_N sembol döner.
    Serbest düşüşteki coinler (-6% altı) elenir.
    """
    candidates = []
    for symbol, ticker in tickers_map.items():
        if not is_valid_symbol(symbol):
            continue
        volume = safe_float(ticker["quoteVolume"])
        if volume < min_volume:
            continue
        change = safe_float(ticker["priceChangePercent"])
        if change < -6:
            continue
        candidates.append((symbol, volume))

    candidates.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in candidates[:PREFILTER_TOP_N]]


def get_klines_df(client: Client, symbol: str) -> Optional[pd.DataFrame]:
    """
    Binance'den OHLCV kline verisi çeker, DataFrame olarak döner.
    Hata veya yetersiz veri durumunda None döner.
    """
    raw = None
    try:
        raw = client.get_klines(
            symbol=symbol,
            interval=KLINE_INTERVAL,
            limit=KLINE_LIMIT,
        )
        if not raw or len(raw) < 100:
            return None

        df = pd.DataFrame(
            raw,
            columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades",
                "taker_buy_base", "taker_buy_quote", "ignore",
            ],
        )
        for col in ("open", "high", "low", "close", "volume", "quote_volume"):
            df[col] = df[col].astype(float)

        return df[["open", "high", "low", "close", "volume", "quote_volume"]].copy()

    except Exception as exc:
        logger.debug("get_klines_df hata %s: %s", symbol, exc)
        return None
    finally:
        del raw


def build_indicators(df: pd.DataFrame) -> Optional[dict]:
    """
    ta kütüphanesi ile RSI, EMA20/50, MACD, Bollinger Bands ve hacim spike
    hesaplar. Tüm ara nesneler işlem sonunda silinerek bellek serbest bırakılır.
    """
    rsi_ind = ema20_ind = ema50_ind = macd_ind = bb_ind = None
    try:
        close = df["close"]
        volume = df["volume"]

        rsi_ind = RSIIndicator(close=close, window=14)
        ema20_ind = EMAIndicator(close=close, window=20)
        ema50_ind = EMAIndicator(close=close, window=50)
        macd_ind = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        bb_ind = BollingerBands(close=close, window=20, window_dev=2)

        def last(series, default=0.0) -> float:
            v = series.iloc[-1]
            return float(v) if not pd.isna(v) else default

        rsi_s = rsi_ind.rsi()
        ema20_s = ema20_ind.ema_indicator()
        ema50_s = ema50_ind.ema_indicator()
        macd_hist_s = macd_ind.macd_diff()
        bb_lower_s = bb_ind.bollinger_lband()
        bb_upper_s = bb_ind.bollinger_hband()

        rsi = last(rsi_s, 50.0)
        ema20 = last(ema20_s)
        ema50 = last(ema50_s)
        macd_hist = last(macd_hist_s)
        macd_prev_hist = float(macd_hist_s.iloc[-2]) if len(macd_hist_s) > 1 and not pd.isna(macd_hist_s.iloc[-2]) else 0.0
        bb_lower = last(bb_lower_s)
        bb_upper = last(bb_upper_s)

        vol_avg = float(volume.tail(20).mean())
        vol_last = float(volume.iloc[-1])
        vol_spike = (vol_last / vol_avg) if vol_avg > 0 else 1.0

        result = {
            "rsi": rsi,
            "ema20": ema20,
            "ema50": ema50,
            "macd_hist": macd_hist,
            "macd_prev_hist": macd_prev_hist,
            "bb_lower": bb_lower,
            "bb_upper": bb_upper,
            "vol_spike": vol_spike,
            "close": float(close.iloc[-1]),
        }

        del rsi_ind, ema20_ind, ema50_ind, macd_ind, bb_ind
        del rsi_s, ema20_s, ema50_s, macd_hist_s, bb_lower_s, bb_upper_s
        del close, volume

        return result

    except Exception as exc:
        logger.debug("build_indicators hata: %s", exc)
        return None


def score_coin(ticker: dict, btc_change: float, ind: dict) -> Optional[dict]:
    """
    TA indikatörleri + ticker verisiyle 0-100 skor üretir.

    Skor bileşenleri (baz 40):
        BTC bağlamı  : -15 / +8
        RSI          : -14 / +18
        EMA trendi   : -5  / +14
        MACD         : -8  / +12  (crossover bonusu dahil)
        Bollinger    : -10 / +10
        Hacim spike  : 0   / +10
        24s değişim  : -12 / +12
    """
    symbol = ticker["symbol"]
    price = safe_float(ticker["lastPrice"])
    change = safe_float(ticker["priceChangePercent"])
    volume = safe_float(ticker["quoteVolume"])

    score = 40

    # BTC bağlamı
    if btc_change < -3:
        score -= 15
    elif btc_change < -1:
        score -= 8
    elif btc_change > 2:
        score += 8
    elif btc_change > 1:
        score += 4

    # RSI
    rsi = ind["rsi"]
    if rsi < 25:
        score += 18
    elif rsi < 35:
        score += 14
    elif rsi < 45:
        score += 8
    elif rsi < 55:
        score += 3
    elif rsi < 65:
        score += 0
    elif rsi < 75:
        score -= 8
    else:
        score -= 14

    # EMA trendi
    ema20 = ind["ema20"]
    ema50 = ind["ema50"]
    close = ind["close"]
    if ema20 > 0 and ema50 > 0:
        if ema20 > ema50:
            score += 10
            if close > ema20:
                score += 4
        else:
            score -= 5

    # MACD histogram
    macd_hist = ind["macd_hist"]
    macd_prev = ind["macd_prev_hist"]
    if macd_hist > 0:
        score += 8
        if macd_prev <= 0:  # taze boğa kesişimi
            score += 4
    elif macd_hist < 0:
        score -= 8

    # Bollinger Bands pozisyonu
    bb_lower = ind["bb_lower"]
    bb_upper = ind["bb_upper"]
    if bb_lower > 0 and bb_upper > 0 and close > 0:
        bb_range = bb_upper - bb_lower
        if bb_range > 0:
            position = (close - bb_lower) / bb_range
            if position < 0.15:
                score += 10
            elif position < 0.30:
                score += 5
            elif position > 0.85:
                score -= 10
            elif position > 0.70:
                score -= 5

    # Hacim spike
    vol_spike = ind["vol_spike"]
    if vol_spike > 3.0:
        score += 10
    elif vol_spike > 2.0:
        score += 7
    elif vol_spike > 1.5:
        score += 4
    elif vol_spike > 1.2:
        score += 2

    # 24s fiyat değişimi momentumu
    if 1.5 <= change <= 5:
        score += 12
    elif 5 < change <= 10:
        score += 6
    elif 10 < change <= 15:
        score -= 2
    elif change > 15:
        score -= 12
    elif 0 <= change < 1.5:
        score += 3
    elif -5 <= change < 0:
        score -= 6
    else:
        score -= 12

    score = max(0, min(score, 100))

    ema_label = "↑ YUKARI" if (ema20 > 0 and ema50 > 0 and ema20 > ema50) else "↓ AŞAĞI"
    macd_label = "BOĞA" if macd_hist > 0 else "AYI"

    return {
        "symbol": symbol,
        "price": price,
        "change": change,
        "volume": volume,
        "score": score,
        "rsi": round(rsi, 1),
        "ema_trend": ema_label,
        "macd": macd_label,
        "vol_spike": round(vol_spike, 2),
    }


def analyze_candidates(
    client: Client,
    candidates: list[str],
    tickers_map: dict,
    btc_change: float,
) -> list[dict]:
    """
    Her aday coin için kline çeker, indikatör hesaplar, skorlar.
    Her DataFrame işlendikten hemen sonra silinerek bellek korunur.
    """
    results = []

    for symbol in candidates:
        ticker = tickers_map.get(symbol)
        if not ticker:
            continue

        df = get_klines_df(client, symbol)
        if df is None:
            continue

        ind = build_indicators(df)
        del df
        gc.collect()

        if ind is None:
            continue

        coin = score_coin(ticker, btc_change, ind)
        if coin:
            results.append(coin)

    results.sort(key=lambda x: x["score"], reverse=True)
    return results
