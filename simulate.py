#!/usr/bin/env python3
"""
Bihter Coin Signal V4 — Günlük Simülasyon
Bugün için sanal portföy üzerinde walk-forward backtest yapar.

Kullanım:
    python3 simulate.py
"""
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from binance.client import Client
from dotenv import load_dotenv

from indicators import build_indicators, score_coin, safe_float

load_dotenv()

# ---------------------------------------------------------------------------
# Simülasyon parametreleri
# ---------------------------------------------------------------------------
STARTING_BALANCE_TL   = 10_000.0
TL_TO_USDT            = 0.0182          # 1 TL ≈ 0.0182 USDT (güncelle gerekirse)
STARTING_BALANCE      = round(STARTING_BALANCE_TL * TL_TO_USDT, 2)  # ~182 USDT

# ── EN İYİ SENARYO parametreleri ─────────────────────────────────────────
MIN_SCORE        = 75           # Sadece güçlü sinyaller (75+)
STOP_LOSS_PCT    = 0.025        # Stop %2.5
TARGET1_PCT      = 0.05         # Hedef 1 %5
TARGET2_PCT      = 0.12         # Hedef 2 %12
TOP_N_COINS      = 20
WARMUP_CANDLES   = 250
TZ_OFFSET_HOURS  = 3

ALLOC_SCORE_80   = 0.30         # 80+ skor → bakiyenin %30'u
ALLOC_SCORE_75   = 0.20         # 75+ skor → bakiyenin %20'si
MAX_OPEN_POS     = 3            # Aynı anda max 3 pozisyon

BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

EXCLUDED = {"BTCUSDT", "ETHUSDT", "BNBUSDT"}
BANNED   = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT", "FDUSD", "TUSD", "USDC"]


# ---------------------------------------------------------------------------
# Yardımcı
# ---------------------------------------------------------------------------

def pos_size(balance: float, score: int) -> float:
    if score >= 80:
        return round(balance * ALLOC_SCORE_80, 2)
    else:
        return round(balance * ALLOC_SCORE_75, 2)


def is_valid(symbol: str) -> bool:
    if not symbol.endswith("USDT") or symbol in EXCLUDED:
        return False
    return not any(b in symbol for b in BANNED)


def fetch_df(client: Client, symbol: str, limit: int) -> pd.DataFrame:
    raw = client.get_klines(symbol=symbol, interval="1h", limit=limit)
    df = pd.DataFrame(raw, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "tbb", "tbq", "ignore"
    ])
    for c in ("open", "high", "low", "close", "volume", "quote_volume"):
        df[c] = df[c].astype(float)
    return df[["open", "high", "low", "close", "volume", "quote_volume"]]


def btc_24h_change(btc_df: pd.DataFrame, idx: int) -> float:
    if idx < 24:
        return 0.0
    curr = btc_df["close"].iloc[idx]
    prev = btc_df["close"].iloc[idx - 24]
    return ((curr - prev) / prev * 100) if prev > 0 else 0.0


def ticker_change_24h(df: pd.DataFrame, idx: int) -> float:
    if idx < 24:
        return 0.0
    curr = df["close"].iloc[idx]
    prev = df["close"].iloc[idx - 24]
    return ((curr - prev) / prev * 100) if prev > 0 else 0.0


def ticker_volume_24h(df: pd.DataFrame, idx: int) -> float:
    start = max(0, idx - 23)
    return float(df["quote_volume"].iloc[start: idx + 1].sum())


# ---------------------------------------------------------------------------
# Simülasyon motoru
# ---------------------------------------------------------------------------

def run_simulation():
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

    now_tr  = datetime.now(timezone.utc) + timedelta(hours=TZ_OFFSET_HOURS)
    sim_end = now_tr.replace(hour=21, minute=0, second=0, microsecond=0)
    if sim_end > now_tr:
        sim_end = now_tr
    sim_start = now_tr.replace(hour=0, minute=0, second=0, microsecond=0)
    hours_today = max(1, int((sim_end - sim_start).total_seconds() / 3600))
    total_candles = WARMUP_CANDLES + hours_today

    print(f"\n{'='*62}")
    print(f"  🚀  BİHETER COİN SİGNAL {'':<4} GÜNLÜK SİMÜLASYON")
    print(f"{'='*62}")
    print(f"  Tarih      : {sim_start.strftime('%d %B %Y')} (Türkiye)")
    print(f"  Süre       : 00:00 → {sim_end.strftime('%H:%M')}")
    print(f"  Başlangıç  : {STARTING_BALANCE_TL:,.0f} TL  ≈  {STARTING_BALANCE:.2f} USDT")
    print(f"  Min skor   : {MIN_SCORE}+")
    print(f"  Stop       : -%{STOP_LOSS_PCT*100:.0f}  |  Hedef1: +%{TARGET1_PCT*100:.0f}  |  Hedef2: +%{TARGET2_PCT*100:.0f}")
    print(f"{'='*62}\n")

    # Top coin listesi
    print("Piyasa verisi çekiliyor...")
    all_tickers = client.get_ticker()
    candidates = sorted(
        [t for t in all_tickers if is_valid(t["symbol"])],
        key=lambda t: safe_float(t["quoteVolume"]),
        reverse=True,
    )[:TOP_N_COINS]
    symbols = [t["symbol"] for t in candidates]

    # BTC verisi (piyasa yönü için)
    btc_df = fetch_df(client, "BTCUSDT", total_candles)

    # Portföy durumu
    balance   = STARTING_BALANCE
    positions = {}   # symbol → {entry, amount, stop, t1, t2, t1_hit, score, tier, open_hour}
    closed    = []
    signals   = []

    print(f"{'─'*62}")
    print(f"  Tarama: {len(symbols)} coin × {hours_today} saat")
    print(f"{'─'*62}\n")

    for sym in symbols:
        df = fetch_df(client, sym, total_candles)
        if len(df) < WARMUP_CANDLES + 2:
            continue
        time.sleep(0.15)

        for i in range(WARMUP_CANDLES, len(df)):
            hour_idx = i - WARMUP_CANDLES   # 0 = bugün gece yarısı
            if hour_idx >= hours_today:
                break

            close_price = float(df["close"].iloc[i])

            # --- Açık pozisyon varsa yönet ---
            if sym in positions:
                pos = positions[sym]

                if close_price >= pos["t2"]:
                    profit = (close_price - pos["entry"]) / pos["entry"] * pos["amount"]
                    balance += pos["amount"] + profit
                    closed.append({
                        "symbol": sym, "result": "🎯 HEDEF 2",
                        "profit": profit,
                        "pct": (close_price - pos["entry"]) / pos["entry"] * 100,
                        "open_h": pos["open_hour"], "close_h": hour_idx,
                    })
                    del positions[sym]

                elif close_price >= pos["t1"] and not pos.get("t1_hit"):
                    pos["t1_hit"] = True
                    half = pos["amount"] / 2
                    profit_half = (close_price - pos["entry"]) / pos["entry"] * half
                    balance += half + profit_half
                    pos["amount"] -= half
                    pos["stop"] = pos["entry"]   # Trailing: stop → giriş

                elif close_price <= pos["stop"]:
                    loss = (close_price - pos["entry"]) / pos["entry"] * pos["amount"]
                    balance += pos["amount"] + loss
                    closed.append({
                        "symbol": sym, "result": "🛑 STOP",
                        "profit": loss,
                        "pct": (close_price - pos["entry"]) / pos["entry"] * 100,
                        "open_h": pos["open_hour"], "close_h": hour_idx,
                    })
                    del positions[sym]

                continue   # Aynı coin için yeni sinyal açma

            # --- Sinyal kontrolü ---
            if len(positions) >= MAX_OPEN_POS:
                continue   # Max pozisyon sayısına ulaşıldı

            df_slice = df.iloc[: i + 1].copy()
            ind = build_indicators(df_slice)
            if ind is None:
                continue

            change_24h = ticker_change_24h(df, i)
            vol_24h    = ticker_volume_24h(df, i)

            fake_ticker = {
                "symbol": sym,
                "lastPrice": str(close_price),
                "priceChangePercent": str(round(change_24h, 4)),
                "quoteVolume": str(vol_24h),
            }

            coin = score_coin(fake_ticker, btc_24h_change(btc_df, i), ind)
            if coin is None or coin["score"] < MIN_SCORE:
                continue

            # Bileşik kazanç: kapanan işlemlerden gelen para yeni pozisyonlara yansır
            amount = pos_size(balance, coin["score"])
            if amount < 5 or amount > balance * 0.95:
                continue

            balance -= amount
            positions[sym] = {
                "entry":     close_price,
                "amount":    amount,
                "stop":      close_price * (1 - STOP_LOSS_PCT),
                "t1":        close_price * (1 + TARGET1_PCT),
                "t2":        close_price * (1 + TARGET2_PCT),
                "score":     coin["score"],
                "tier":      coin["tier"],
                "open_hour": hour_idx,
                "t1_hit":    False,
            }
            signals.append({
                "symbol": sym, "score": coin["score"],
                "tier": coin["tier"], "entry": close_price,
                "hour": hour_idx,
                "rsi": coin["rsi"], "ema": coin["ema_trend"], "macd": coin["macd"],
            })
            print(
                f"  ⚡ Saat {hour_idx:02d}:00  {sym:<12} Skor:{coin['score']:>3}  "
                f"{coin['tier']:<16} Giriş:{close_price:.4f}  Tutar:{amount:.0f} USDT"
            )

    # Gün sonu: açık pozisyonları kapat
    print()
    for sym, pos in list(positions.items()):
        last = fetch_df(client, sym, 2)
        time.sleep(0.1)
        last_price = float(last["close"].iloc[-1])
        profit = (last_price - pos["entry"]) / pos["entry"] * pos["amount"]
        balance += pos["amount"] + profit
        closed.append({
            "symbol": sym, "result": "⏰ AÇIK (gün sonu)",
            "profit": profit,
            "pct": (last_price - pos["entry"]) / pos["entry"] * 100,
            "open_h": pos["open_hour"], "close_h": hours_today,
        })
        del positions[sym]

    # ---------------------------------------------------------------------------
    # Rapor
    # ---------------------------------------------------------------------------
    total_pnl     = balance - STARTING_BALANCE
    total_pnl_pct = total_pnl / STARTING_BALANCE * 100
    wins          = [c for c in closed if c["profit"] > 0]
    losses        = [c for c in closed if c["profit"] <= 0]
    win_rate      = len(wins) / len(closed) * 100 if closed else 0

    print(f"\n{'='*62}")
    print(f"  📊  SİMÜLASYON SONUÇLARI")
    print(f"{'='*62}")
    tl_pnl  = round(total_pnl / TL_TO_USDT, 0)
    pnl_sign = "+" if total_pnl >= 0 else ""
    emoji = "🟢" if total_pnl >= 0 else "🔴"
    print(f"  Başlangıç   : {STARTING_BALANCE_TL:>8,.0f} TL  ({STARTING_BALANCE:.2f} USDT)")
    print(f"  Toplam PnL  : {emoji} {pnl_sign}{tl_pnl:>6,.0f} TL  ({pnl_sign}{total_pnl:.2f} USDT)  [{pnl_sign}{total_pnl_pct:.2f}%]")
    print(f"{'─'*62}")
    print(f"  Toplam sinyal     : {len(signals)}")
    print(f"  Kapatılan işlem   : {len(closed)}")
    print(f"  Kazanan           : {len(wins)}   Kaybeden: {len(losses)}")
    if closed:
        print(f"  Kazanma oranı     : %{win_rate:.0f}")

    if signals:
        print(f"\n{'─'*62}")
        print("  AL SİNYALLERİ:")
        for s in signals:
            print(
                f"    Saat {s['hour']:02d}:00  {s['symbol']:<12} "
                f"Skor:{s['score']}  {s['tier']:<16} "
                f"RSI:{s['rsi']:.0f}  EMA:{s['ema']}  MACD:{s['macd']}  "
                f"Giriş:{s['entry']:.4f}"
            )

    if closed:
        print(f"\n{'─'*62}")
        print("  KAPANAN İŞLEMLER:")
        for c in closed:
            pct_str = f"{c['pct']:+.2f}%"
            pnl_str = f"{c['profit']:+.2f} USDT"
            print(
                f"    {c['result']:<20} {c['symbol']:<12} "
                f"{pnl_str:>12}  ({pct_str:>8})  "
                f"Saat {c['open_h']:02d}:00 → {c['close_h']:02d}:00"
            )

    print(f"\n{'='*62}")
    print("  ⚠️  Bu simülasyon geçmiş veriye dayalıdır.")
    print("     Gelecekteki performansı garanti etmez.")
    print(f"{'='*62}\n")

    return balance, signals, closed


if __name__ == "__main__":
    run_simulation()
