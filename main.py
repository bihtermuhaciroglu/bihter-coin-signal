import gc
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

from indicators import (
    analyze_candidates,
    prefilter_candidates,
    safe_float,
    get_klines_df,
    build_indicators,
    score_coin,
    is_valid_symbol,
    compute_atr_stop,
    get_btc_market_strength,
)
from trader import (
    market_buy,
    market_sell,
    sell_all_to_usdt,
    get_active_session,
    start_session,
    end_session,
    analyze_trade_history,
    format_analysis_report,
)
from learner import build_nightly_report, market_intel
from ai_brain import (
    ask_ai,
    ai_coin_comment,
    btc_tracker,
    circuit_breaker,
    detect_flash_crash,
    compute_portfolio_drawdown,
    volatility_position_multiplier,
    compute_atr_ratio,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

SCAN_INTERVAL = 180 if os.getenv("AUTO_TRADE", "false").lower() == "true" else 300
REPORT_INTERVAL = 3600        # 1 saat
SIGNAL_COOLDOWN_HOURS = 6     # Aynı coin için minimum bekleme
SIGNAL_RESEND_SCORE_DELTA = 8 # Cooldown içinde yeniden göndermek için gereken skor artışı
MIN_VOLUME_USDT = 400_000     # Momentum coinler — düşük hacimli ama likit
STATE_FILE = "state.json"

MAX_NEW_SIGNALS = 3            # Aynı anda max 3 yeni alım
MAX_OPEN_POSITIONS = 3         # Toplam açık pozisyon limiti
MIN_BUY_SCORE = 82             # Sadece gerçekten güçlü sinyaller

# Render Environment Variables'tan opsiyonel olarak set et:
# PORTFOLIO_SIZE_USDT=182  → gerçek bakiye düşükse bunu kullan
_env_portfolio = os.getenv("PORTFOLIO_SIZE_USDT")
PORTFOLIO_SIZE_USDT = float(_env_portfolio) if _env_portfolio else None

STOP_LOSS_PCT = 0.10           # Stop %10 — geniş, erken stop yemez
AUTO_STOP_LOSS_PCT = 0.10      # Otomatik modda da %10 stop
TARGET1_PCT = 0.12             # Hedef 1 %12 (sinyal modu)
TARGET2_PCT = 0.25             # Hedef 2 %25 (sinyal modu)

# Otomatik işlem modu
AUTO_TARGET1_PCT = float(os.getenv("AUTO_TARGET1_PCT", "0.08"))    # %8
AUTO_TARGET2_PCT = float(os.getenv("AUTO_TARGET2_PCT", "0.18"))    # %18
QUICK_PROFIT_PCT = float(os.getenv("QUICK_PROFIT_PCT", "0.05"))    # %5 hızlı çık
MAX_HOLD_HOURS = float(os.getenv("MAX_HOLD_HOURS", "6"))           # 6 saat bekle
MIN_PROFIT_AFTER_HOLD = float(os.getenv("MIN_PROFIT_AFTER_HOLD", "0.03"))  # %3
STALE_LOSS_HOURS = float(os.getenv("STALE_LOSS_HOURS", "8"))       # 8 saat ekside kal
STALE_LOSS_PCT = float(os.getenv("STALE_LOSS_PCT", "-6"))          # %-6 ekside kes
ROTATION_EXIT_SCORE = int(os.getenv("ROTATION_EXIT_SCORE", "65"))  # Skor 65 altı = çık
FLAT_EXIT_HOURS = float(os.getenv("FLAT_EXIT_HOURS", "3"))         # 3 saat yatay = çık
FLAT_EXIT_MAX_PNL = float(os.getenv("FLAT_EXIT_MAX_PNL", "1.0"))   # %1 altı kâr yatay
BINANCE_FEE_PCT = 0.001        # tek yön %0.1

BTC_PAUSE_THRESHOLD = -2.0     # BTC bu kadar düşünce yeni sinyal gönderme
BTC_STRONG_MARKET = 1.5        # BTC bu kadar artınca piyasa sağlıklı

MAX_CLOSED_SIGNALS = 50
MAX_SIGNAL_HISTORY = 100

VERSION = "V6"

# Otomatik işlem — gerçek emir gönderir
AUTO_TRADE_ENABLED = os.getenv("AUTO_TRADE", "false").lower() == "true"

# Gece öğrenme saati (UTC saat 21 = TR 00:00)
NIGHTLY_LEARN_HOUR_UTC = 21

# Piyasa zekası güncelleme aralığı (saniye)
INTEL_REFRESH_INTERVAL = 1800  # 30 dakika

# analyze_candidates eş zamanlı çalışmasını engeller (main loop + /portfolio)
_analysis_lock = threading.Semaphore(1)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

_tg_sent_cache: dict = {}   # mesaj_hash → son_gönderim_timestamp
_TG_DEDUP_SECONDS = 300     # Aynı mesaj 5 dk içinde tekrar gönderilmez
_TG_RATE_WINDOW   = 60      # saniye
_TG_RATE_LIMIT    = 20      # pencerede max mesaj sayısı
_tg_rate_times: list = []   # son gönderim zamanları

def send_telegram(text: str, reply_markup: dict = None, dedup: bool = True) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("Telegram kimlik bilgileri eksik.")
        return

    now = time.time()

    # Hız limiti — dakikada 20 mesaj
    _tg_rate_times[:] = [t for t in _tg_rate_times if now - t < _TG_RATE_WINDOW]
    if len(_tg_rate_times) >= _TG_RATE_LIMIT:
        logger.warning("Telegram hız limiti — mesaj atlandı.")
        return
    _tg_rate_times.append(now)

    # Tekrar önleme — aynı mesaj 5 dk içinde bir kez
    if dedup:
        import hashlib
        key = hashlib.md5(text[:200].encode()).hexdigest()
        last_sent = _tg_sent_cache.get(key, 0)
        if now - last_sent < _TG_DEDUP_SECONDS:
            return
        _tg_sent_cache[key] = now
        # Cache büyümesin
        if len(_tg_sent_cache) > 500:
            oldest = sorted(_tg_sent_cache, key=lambda k: _tg_sent_cache[k])
            for k in oldest[:100]:
                del _tg_sent_cache[k]

    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Telegram gönderi hatası: %s", exc)


# ---------------------------------------------------------------------------
# State yönetimi
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(STATE_FILE, "r") as fh:
            s = json.load(fh)
    except Exception:
        s = {}
    s.setdefault("signals", {})
    s.setdefault("last_report", 0)
    s.setdefault("closed", [])
    s.setdefault("trades", [])           # gerçek işlem geçmişi
    s.setdefault("last_nightly", "")     # son gece öğrenme tarihi
    s.setdefault("adaptive", {           # dinamik parametreler
        "min_score": MIN_BUY_SCORE,
        "stop_pct":  STOP_LOSS_PCT,
    })
    return s


def save_state(state: dict) -> None:
    _prune_state(state)
    try:
        with open(STATE_FILE, "w") as fh:
            json.dump(state, fh, indent=2)
    except Exception as exc:
        logger.error("State kaydetme hatası: %s", exc)


def _prune_state(state: dict) -> None:
    closed = state.get("closed", [])
    if len(closed) > MAX_CLOSED_SIGNALS:
        state["closed"] = closed[-MAX_CLOSED_SIGNALS:]

    signals = state.get("signals", {})
    if len(signals) > MAX_SIGNAL_HISTORY:
        sorted_items = sorted(signals.items(), key=lambda kv: kv[1].get("time", ""))
        for symbol, _ in sorted_items[: len(signals) - MAX_SIGNAL_HISTORY]:
            del signals[symbol]


# ---------------------------------------------------------------------------
# Binance veri
# ---------------------------------------------------------------------------

def get_balances(client: Client) -> dict:
    account = client.get_account()
    balances = {}
    for item in account["balances"]:
        asset = item["asset"]
        total = safe_float(item["free"]) + safe_float(item["locked"])
        if total > 0:
            balances[asset] = total
    del account
    return balances


def get_usdt_balance(balances: dict) -> float:
    return balances.get("USDT", 0.0)


def get_tickers_map(client: Client) -> dict:
    tickers = client.get_ticker()
    result = {item["symbol"]: item for item in tickers}
    del tickers
    return result


# ---------------------------------------------------------------------------
# Pozisyon boyutu (risk-bazlı)
# ---------------------------------------------------------------------------

def position_size(usdt_balance: float, score: int) -> dict:
    """
    Agresif pozisyon boyutu — yüksek skor = büyük pozisyon.

    Skor  95+  → bakiyenin %95'i  (neredeyse tümü — çok nadir)
    Skor  92+  → bakiyenin %75'i
    Skor  89+  → bakiyenin %55'i
    Skor  86+  → bakiyenin %40'ı
    Skor  83+  → bakiyenin %25'i
    Skor  80+  → bakiyenin %15'i
    """
    if usdt_balance <= 0 or score < MIN_BUY_SCORE:
        return {"amount": 0.0, "alloc_pct": 0, "portfolio_risk_pct": 0.0}

    if score >= 92:
        alloc_pct = 0.40
    elif score >= 89:
        alloc_pct = 0.30
    elif score >= 86:
        alloc_pct = 0.20
    else:
        alloc_pct = 0.15

    amount = round(usdt_balance * alloc_pct, 2)
    max_loss = round(amount * STOP_LOSS_PCT, 2)
    portfolio_risk = round((max_loss / usdt_balance) * 100, 2) if usdt_balance > 0 else 0.0

    return {
        "amount": amount,
        "alloc_pct": int(alloc_pct * 100),
        "portfolio_risk_pct": portfolio_risk,
    }


# ---------------------------------------------------------------------------
# Spam önleme
# ---------------------------------------------------------------------------

def should_send_new_signal(
    symbol: str, score: int, state: dict, balances: dict
) -> bool:
    asset = symbol.replace("USDT", "")
    if asset in balances and balances[asset] > 0:
        return False  # Zaten sahip olduğun coini tekrar önermez

    existing = state["signals"].get(symbol)
    if existing:
        last_time = datetime.fromisoformat(existing["time"])
        elapsed = datetime.now(timezone.utc).replace(tzinfo=None) - last_time
        if elapsed < timedelta(hours=SIGNAL_COOLDOWN_HOURS):
            if score < existing["score"] + SIGNAL_RESEND_SCORE_DELTA:
                return False  # Cooldown içinde, skor yeterince artmadı
    return True


# ---------------------------------------------------------------------------
# Sinyal oluşturma
# ---------------------------------------------------------------------------

def _target_pcts() -> tuple[float, float]:
    """Otomatik işlemde daha düşük hedefler kullan."""
    if AUTO_TRADE_ENABLED:
        return AUTO_TARGET1_PCT, AUTO_TARGET2_PCT
    return TARGET1_PCT, TARGET2_PCT


def _effective_stop_pct() -> float:
    if AUTO_TRADE_ENABLED:
        return AUTO_STOP_LOSS_PCT
    return STOP_LOSS_PCT


def _count_open_positions(state: dict, balances: dict, tickers_map: dict = None) -> int:
    """Aktif sinyal say. 5 USDT altındaki toz bakiyeler sayılmaz."""
    seen = set()
    for sym, sig in state.get("signals", {}).items():
        if sig.get("status") == "active":
            seen.add(sym)
    for asset, qty in balances.items():
        if asset == "USDT" or qty <= 0:
            continue
        sym   = asset + "USDT"
        price = 0.0
        if tickers_map:
            t = tickers_map.get(sym)
            if t:
                price = safe_float(t["lastPrice"])
        value = qty * price if price else 999
        if value >= MIN_SYNC_VALUE_USDT:
            seen.add(sym)
    return len(seen)


MIN_SYNC_VALUE_USDT = 5.0   # Toz pozisyonları yoksay (<5 USDT değeri)

def _sync_holdings_to_state(balances: dict, state: dict, tickers_map: dict) -> None:
    """
    Binance'deki coinleri state'e bağla — satış takibi kaçırılmasın.
    5 USDT altındaki toz pozisyonları yoksayar.
    """
    now_str = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    for asset, qty in balances.items():
        if asset == "USDT" or qty <= 0:
            continue
        symbol = asset + "USDT"
        ticker = tickers_map.get(symbol)
        if not ticker:
            continue
        price = safe_float(ticker["lastPrice"])
        value = qty * price

        sig = state["signals"].get(symbol)
        if sig and sig.get("status") == "active":
            if not sig.get("auto_qty") or sig["auto_qty"] <= 0:
                sig["auto_qty"] = qty
            continue

        # Toz miktarlar — senkronize etme, durumu kirletmesin
        if value < MIN_SYNC_VALUE_USDT:
            continue

        # Kayıtsız ama anlamlı pozisyon — takibe al
        state["signals"][symbol] = {
            "symbol":     symbol,
            "status":     "active",
            "time":       now_str,
            "entry":      price,
            "auto_price": price,
            "auto_qty":   qty,
            "auto_cost":  round(value, 4),
            "amount":     round(value, 2),
            "score":      0,
            "orphan":     True,
            "stop":       round(price * (1 - _effective_stop_pct()), 8),
            "target1":    round(price * (1 + _target_pcts()[0]), 8),
            "target2":    round(price * (1 + _target_pcts()[1]), 8),
        }
        logger.info("Kayıtsız pozisyon takibe alındı: %s qty=%.6f değer=%.2f USDT",
                    symbol, qty, value)


def _effective_entry(signal: dict) -> float:
    return signal.get("auto_price") or signal.get("entry") or 0.0


def _apply_exit_levels(signal: dict, entry: float, stop_pct: float) -> None:
    t1, t2 = _target_pcts()
    signal["entry"] = entry
    signal["stop"] = round(entry * (1 - stop_pct), 8)
    signal["target1"] = round(entry * (1 + t1), 8)
    signal["target2"] = round(entry * (1 + t2), 8)


def create_signal(coin: dict, usdt_balance: float) -> dict:
    entry = coin["price"]
    pos = position_size(usdt_balance, coin["score"])
    stop_pct = _effective_stop_pct()
    t1, t2 = _target_pcts()
    return {
        "symbol": coin["symbol"],
        "time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "entry": entry,
        "stop": round(entry * (1 - stop_pct), 8),
        "target1": round(entry * (1 + t1), 8),
        "target2": round(entry * (1 + t2), 8),
        "score": coin["score"],
        "tier": coin.get("tier", ""),
        "amount": pos["amount"],
        "alloc_pct": pos["alloc_pct"],
        "portfolio_risk_pct": pos["portfolio_risk_pct"],
        "status": "active",
        "rsi": coin.get("rsi", 0),
        "ema_trend": coin.get("ema_trend", "-"),
        "macd": coin.get("macd", "-"),
        "vol_spike": coin.get("vol_spike", 1.0),
        "roc3": coin.get("roc3", 0.0),
    }


def _signal_strength_label(score: int) -> str:
    """Skoru kullanıcı dostu güç etiketine çevirir."""
    if score >= 90:
        return "🔥 Çok Güçlü Sinyal"
    if score >= 85:
        return "💪 Güçlü Sinyal"
    return "✅ Geçerli Sinyal"


def send_buy_signals(new_signals: list, usdt_balance: float, effective_usdt: float = None) -> None:
    eff = effective_usdt or usdt_balance

    if usdt_balance < 10:
        send_telegram(
            f"⚠️ Bakiyen çok düşük ({usdt_balance:.2f} USDT) — işlem yapılamaz.\n"
            "Anlamlı alım için en az 50 USDT gerekli."
        )
        return

    for sig in new_signals:
        strength = _signal_strength_label(sig["score"])
        coin_name = sig["symbol"].replace("USDT", "")
        entry  = sig["entry"]
        stop   = sig["stop"]
        t1     = sig["target1"]
        t2     = sig["target2"]
        amount = sig["amount"]
        risk_usd = round(amount * STOP_LOSS_PCT, 2)
        gain1_usd = round(amount * TARGET1_PCT, 2)
        gain2_usd = round(amount * TARGET2_PCT, 2)

        intel_bonus = sig.get("intel_bonus", 0)
        bonus_str   = f"  (piyasa zekası: {intel_bonus:+d})" if intel_bonus != 0 else ""

        # Piyasa durumu kısa özet
        intel_line = market_intel.summary_text().split("\n")[0] if not market_intel.is_stale() else ""

        breakeven_price  = round(entry * 1.00, 6)
        lock5_price      = round(entry * 1.05, 6)
        lock10_price     = round(entry * 1.10, 6)

        lines = [
            f"🚨 {coin_name} — AL SİNYALİ  {strength}",
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"",
            f"✅ {entry:.6f} fiyatından AL",
            f"   👉 {amount} USDT harca{bonus_str}",
            f"",
            f"🛑 {stop:.6f} fiyatına düşerse SAT  (−{risk_usd:.2f} USDT / -%{AUTO_STOP_LOSS_PCT*100:.0f})",
            f"🎯 {t1:.6f} fiyatına gelince SAT  (+{gain1_usd:.2f} USDT) ← 1. hedef",
            f"🚀 {t2:.6f} fiyatına gelince SAT  (+{gain2_usd:.2f} USDT) ← 2. hedef",
            f"",
            f"📈 HAREKETLİ STOP:",
            f"   %5 kârda  → stop {breakeven_price:.6f} (zarar etmezsin)",
            f"   %10 kârda → stop {lock5_price:.6f} (+%5 garanti)",
            f"   %15 kârda → stop {lock10_price:.6f} (+%10 garanti)",
            f"",
            f"📊 Bakiyen: {usdt_balance:.2f} USDT  |  Bu işlem bakiyenin %{sig['alloc_pct']}'i",
        ]
        if intel_line:
            lines.append(intel_line)

        buttons = [[
            {"text": f"✅ Aldım", "callback_data": f"bought_{sig['symbol']}"},
            {"text": "❌ Atladım", "callback_data": f"skip_{sig['symbol']}"},
        ]]
        send_telegram("\n".join(lines), reply_markup={"inline_keyboard": buttons})


# ---------------------------------------------------------------------------
# Aktif sinyal takibi
# ---------------------------------------------------------------------------

def check_active_signals(
    state: dict,
    tickers_map: dict,
    client: Client = None,
    scored: list = None,
    eff_min_score: float = None,
) -> None:
    now_dt = datetime.now(timezone.utc).replace(tzinfo=None)
    score_map = {c["symbol"]: c for c in (scored or [])}
    if eff_min_score is None:
        eff_min_score = state.get("adaptive", {}).get("min_score", MIN_BUY_SCORE)

    for symbol, signal in list(state["signals"].items()):
        if signal.get("status") not in ("active",):
            continue

        ticker = tickers_map.get(symbol)
        if not ticker:
            continue

        price = safe_float(ticker["lastPrice"])
        entry = _effective_entry(signal)
        if entry <= 0:
            continue

        pnl = ((price - entry) / entry) * 100
        net_pnl = pnl - (BINANCE_FEE_PCT * 2 * 100)  # al+s sat komisyonu

        now_str   = now_dt.isoformat()
        coin_name = symbol.replace("USDT", "")
        amount    = signal.get("amount", 0)
        pnl_usd   = round(amount * pnl / 100, 2)

        opened = datetime.fromisoformat(signal["time"])
        hold_hours = (now_dt - opened).total_seconds() / 3600

        # ── Zayıf sinyal / zaman aşımı çıkışları (otomatik mod) ──────────
        if AUTO_TRADE_ENABLED and client:
            coin_score = score_map.get(symbol, {}).get("score")

            # Yatay piyasa — 45 dk geçti, kâr yok → sat, parayı serbest bırak
            if hold_hours >= FLAT_EXIT_HOURS and -0.5 <= pnl <= FLAT_EXIT_MAX_PNL:
                signal["status"] = "flat_exit"
                signal["time"] = now_str
                send_telegram(
                    f"📤 {coin_name} — YATAY, SATILIYOR\n\n"
                    f"{hold_hours:.1f} saat geçti, fiyat neredeyse aynı (%{pnl:.1f}).\n"
                    f"Başka fırsata geçiyoruz."
                )
                _auto_execute_sell(client, symbol, state, "flat_exit")
                continue

            # Coin rotasyonu: elindeki coin zayıflarken daha iyi fırsat varsa geç
            best_opportunity = max(score_map.values(), key=lambda c: c["score"]) if score_map else None
            if (best_opportunity
                    and best_opportunity["symbol"] != symbol
                    and best_opportunity["score"] >= eff_min_score
                    and best_opportunity["score"] - (coin_score or 0) >= 15
                    and pnl >= -1.0   # zararı çok büyük değilse rotasyon yap
                    and not signal.get("pp1_done")):  # hiç kısmi kâr alınmamışsa
                signal["status"] = "rotation"
                signal["time"]   = now_str
                best_name = best_opportunity["symbol"].replace("USDT", "")
                send_telegram(
                    f"🔄 {coin_name} → {best_name} ROTASYON\n\n"
                    f"{coin_name} skoru: {coin_score}/100\n"
                    f"{best_name} skoru: {best_opportunity['score']}/100\n"
                    f"Fark +{best_opportunity['score']-(coin_score or 0)} puan — geçmek mantıklı.\n"
                    f"PnL: {pnl_usd:+.2f} USDT (%{pnl:.1f})"
                )
                _auto_execute_sell(client, symbol, state, "rotation")
                continue

            if coin_score is not None and coin_score < ROTATION_EXIT_SCORE and pnl < 2.0:
                signal["status"] = "rotation"
                signal["time"] = now_str
                send_telegram(
                    f"🔄 {coin_name} — ZAYIF GÖRÜNÜM, SATILIYOR\n\n"
                    f"Skor düştü ({coin_score}/100), momentum zayıf.\n"
                    f"PnL: {pnl_usd:+.2f} USDT (%{pnl:.1f})"
                )
                _auto_execute_sell(client, symbol, state, "rotation")
                continue

            if hold_hours >= STALE_LOSS_HOURS and pnl <= STALE_LOSS_PCT:
                signal["status"] = "stale_loss"
                signal["time"] = now_str
                send_telegram(
                    f"⏰ {coin_name} — EKSİDE KALDI, SATILIYOR\n\n"
                    f"{hold_hours:.1f} saat geçti, %{pnl:.1f}.\n"
                    f"Zararı büyütmeden çıkıyoruz."
                )
                _auto_execute_sell(client, symbol, state, "stale_loss")
                continue

            if hold_hours >= MAX_HOLD_HOURS and net_pnl >= MIN_PROFIT_AFTER_HOLD * 100:
                signal["status"] = "time_profit"
                signal["time"] = now_str
                send_telegram(
                    f"⏰ {coin_name} — SÜRE DOLDU, KÂRLA SATILIYOR\n\n"
                    f"{hold_hours:.1f} saat tutuldu.\n"
                    f"Net kâr (komisyon sonrası): ~%{net_pnl:.1f}"
                )
                _auto_execute_sell(client, symbol, state, "time_profit")
                continue

            if net_pnl >= QUICK_PROFIT_PCT * 100 and not signal.get("quick_sold"):
                signal["quick_sold"] = True
                signal["status"] = "quick_profit"
                signal["time"] = now_str
                send_telegram(
                    f"💰 {coin_name} — HIZLI KÂR ALINDI\n\n"
                    f"%{pnl:.1f} yükseldi → komisyon sonrası net kâr kilitlendi.\n"
                    f"Tahmini: +{pnl_usd:.2f} USDT"
                )
                _auto_execute_sell(client, symbol, state, "quick_profit")
                continue

        # ── Hareketli Stop (Trailing Stop) ───────────────────────────────
        #
        #  Kâr %5+  → stop = giriş fiyatı       (zarar etmezsin)
        #  Kâr %10+ → stop = giriş × 1.05        (en az %5 kâr garanti)
        #  Kâr %15+ → stop = giriş × 1.10        (en az %10 kâr garanti)
        #  Kâr %20+ sonrası her %5'te stop %3 yukarı
        #
        new_stop = signal["stop"]

        if pnl >= 20 and signal.get("trailing_level", 0) < 4:
            new_stop = max(signal["stop"], round(entry * 1.12, 8))
            if new_stop > signal["stop"]:
                signal["trailing_level"] = 4
        elif pnl >= 15 and signal.get("trailing_level", 0) < 3:
            new_stop = max(signal["stop"], round(entry * 1.10, 8))
            if new_stop > signal["stop"]:
                signal["trailing_level"] = 3
        elif pnl >= 10 and signal.get("trailing_level", 0) < 2:
            new_stop = max(signal["stop"], round(entry * 1.05, 8))
            if new_stop > signal["stop"]:
                signal["trailing_level"] = 2
        elif pnl >= 5 and signal.get("trailing_level", 0) < 1:
            new_stop = max(signal["stop"], round(entry * 1.00, 8))  # breakeven
            if new_stop > signal["stop"]:
                signal["trailing_level"] = 1

        # Dinamik trailing: %15+ kârdan sonra her %5'te stop %3 yukarı
        if pnl >= 15:
            dynamic_stop = round(entry * (1 + (pnl - 8) / 100 * 0.60), 8)
            new_stop = max(new_stop, dynamic_stop)

        # Stop yükseldiyse kaydet ve bildir
        if new_stop > signal["stop"]:
            old_stop  = signal["stop"]
            signal["stop"]         = new_stop
            signal["stop_raised"]  = True
            guaranteed_pct = (new_stop - entry) / entry * 100
            guaranteed_usd = round(amount * guaranteed_pct / 100, 2)
            g_str = (
                f"+{guaranteed_usd:.2f} USDT kâr garantilendi (%{guaranteed_pct:.1f})"
                if guaranteed_pct > 0 else
                "zarar sıfırlandı (breakeven)"
            )
            send_telegram(
                f"📈 {coin_name} — HAREKETLİ STOP YÜKSELDİ\n\n"
                f"Kâr    : %{pnl:.1f}  (+{pnl_usd:.2f} USDT)\n"
                f"Eski stop : {old_stop:.6f}\n"
                f"Yeni stop : {new_stop:.6f}  ← {g_str}"
            )
        # ─────────────────────────────────────────────────────────────────

        # ── Partial Profit Taking ──────────────────────────────────────────
        # +6% → %30 sat | +10% → %30 sat | +15% → %20 sat | kalan %20 trailing
        qty_now = signal.get("auto_qty", 0)

        if pnl >= 15 and not signal.get("pp3_done") and qty_now > 0:
            signal["pp3_done"] = True
            sell_qty = round(qty_now * 0.20, 8)
            partial_usd = round(amount * 0.20 * pnl / 100, 2)
            send_telegram(
                f"💰 {coin_name} — KISMİ KÂR 3/3 (+%15)\n"
                f"Pozisyonun %20'si satılıyor. Kalan %20 trailing stop'ta.\n"
                f"Tahmini: +{partial_usd:.2f} USDT"
            )
            if client and AUTO_TRADE_ENABLED:
                market_sell(client, symbol, sell_qty)
                signal["auto_qty"] = max(0, qty_now - sell_qty)

        elif pnl >= 10 and not signal.get("pp2_done") and qty_now > 0:
            signal["pp2_done"] = True
            sell_qty = round(qty_now * 0.30, 8)
            partial_usd = round(amount * 0.30 * pnl / 100, 2)
            send_telegram(
                f"💰 {coin_name} — KISMİ KÂR 2/3 (+%10)\n"
                f"Pozisyonun %30'u satılıyor.\n"
                f"Tahmini: +{partial_usd:.2f} USDT"
            )
            if client and AUTO_TRADE_ENABLED:
                market_sell(client, symbol, sell_qty)
                signal["auto_qty"] = max(0, qty_now - sell_qty)

        elif pnl >= 6 and not signal.get("pp1_done") and qty_now > 0:
            signal["pp1_done"] = True
            sell_qty = round(qty_now * 0.30, 8)
            partial_usd = round(amount * 0.30 * pnl / 100, 2)
            send_telegram(
                f"💰 {coin_name} — KISMİ KÂR 1/3 (+%6)\n"
                f"Pozisyonun %30'u satılıyor.\n"
                f"Tahmini: +{partial_usd:.2f} USDT"
            )
            if client and AUTO_TRADE_ENABLED:
                market_sell(client, symbol, sell_qty)
                signal["auto_qty"] = max(0, qty_now - sell_qty)
        # ─────────────────────────────────────────────────────────────────

        if price >= signal["target2"]:
            signal["status"] = "target2"
            signal["time"]   = now_str
            send_telegram(
                f"🎯 {coin_name} — 2. HEDEFE ULAŞTI!\n\n"
                f"Kalan pozisyon satılıyor.\n"
                f"💰 Tahmini kâr: +{pnl_usd:.2f} USDT  (%{pnl:.1f})"
            )
            if client:
                _auto_execute_sell(client, symbol, state, "target2")

        elif price <= signal["stop"]:
            signal["status"] = "stopped"
            signal["time"]   = now_str
            level = signal.get("trailing_level", 0)
            if level >= 1:
                send_telegram(
                    f"🔒 {coin_name} — TRAILING STOP TETİKLENDİ\n\n"
                    f"Kısmi kârlar zaten alındı, kalan trailing stop'ta kapandı.\n"
                    f"Son PnL: {pnl_usd:+.2f} USDT  (%{pnl:.1f})"
                )
            else:
                send_telegram(
                    f"🛑 {coin_name} — ZARAR KES TETİKLENDİ\n\n"
                    f"Pozisyonu sat, zararı büyütme.\n"
                    f"💸 Tahmini zarar: {pnl_usd:.2f} USDT  (%{pnl:.1f})"
                )
            if client:
                _auto_execute_sell(client, symbol, state, "stop")


# ---------------------------------------------------------------------------
# Portföy rotasyon motoru
# ---------------------------------------------------------------------------

def _rotation_action(score: int, pnl_pct: float = None) -> str:
    """Skora ve PnL'e göre öneri etiketi üretir."""
    if score >= 80:
        return "✅ TUT"
    elif score >= 75:
        if pnl_pct is not None and pnl_pct > 5:
            return "📈 STOP YÜKSELT"
        return "👁 İZLE"
    elif score >= 65:
        if pnl_pct is not None and pnl_pct > 3:
            return "💰 KÂR AL (kısmi)"
        return "⚠️ POZİSYON AZALT"
    else:
        return "🔄 BAŞKA COİNE GEÇ"


def _plain_action(score: int, pnl_pct: float = None) -> str:
    """Skora ve PnL'e göre sade Türkçe aksiyon metni üretir."""
    if score >= 80:
        if pnl_pct is not None and pnl_pct >= 10:
            return "Yarısını sat, kârını koru"
        return "Tut — hâlâ güçlü"
    elif score >= 75:
        if pnl_pct is not None and pnl_pct > 5:
            return "Stop-loss'u giriş fiyatına çek"
        return "İzle — henüz sat değil"
    elif score >= 65:
        if pnl_pct is not None and pnl_pct > 3:
            return "Kısmi kâr al, bir miktar sat"
        return "Pozisyonu azalt"
    else:
        return "Sat — daha iyi fırsat var"


def portfolio_report(balances: dict, scored: list, state: dict = None) -> None:
    usdt = get_usdt_balance(balances)
    top3 = scored[:3]

    lines = [
        f"📊 PORTFÖY RAPORU",
        f"💰 Kullanılabilir USDT: {usdt:.2f}\n",
    ]

    # En iyi fırsatlar
    if top3:
        lines.append("🔥 Şu an en iyi fırsatlar:")
        for idx, coin in enumerate(top3, 1):
            coin_name = coin["symbol"].replace("USDT", "")
            pos = position_size(usdt, coin["score"])
            strength = _signal_strength_label(coin["score"])
            lines.append(
                f"  {idx}) {coin_name}  —  {strength}\n"
                f"      Önerilen tutar: {pos['amount']} USDT  |  24 saatlik değişim: %{coin['change']:+.1f}"
            )

    # Elindeki coinleri değerlendir
    holdings = []
    for asset, amount in balances.items():
        if asset == "USDT":
            continue
        sym = asset + "USDT"
        match = next((c for c in scored if c["symbol"] == sym), None)
        if match:
            pnl_pct = None
            if state:
                sig = state.get("signals", {}).get(sym)
                if sig and sig.get("entry", 0) > 0:
                    current = match["price"]
                    pnl_pct = ((current - sig["entry"]) / sig["entry"]) * 100
            holdings.append({**match, "amount": amount, "pnl_pct": pnl_pct})

    if holdings:
        holdings.sort(key=lambda x: x["score"], reverse=True)
        lines.append("\n📌 Elindeki coinler için ne yapmalısın:")
        for h in holdings:
            coin_name = h["symbol"].replace("USDT", "")
            action = _plain_action(h["score"], h.get("pnl_pct"))
            pnl_str = f"  (Kâr/Zarar: %{h['pnl_pct']:+.1f})" if h.get("pnl_pct") is not None else ""
            lines.append(f"  • {coin_name}{pnl_str}\n    👉 {action}")

        # Rotasyon önerisi
        weakest = holdings[-1]
        strongest = top3[0] if top3 else None
        if strongest and strongest["score"] - weakest["score"] >= 20:
            w_name = weakest["symbol"].replace("USDT", "")
            s_name = strongest["symbol"].replace("USDT", "")
            lines += [
                f"\n🔄 ROTASYON ÖNERİSİ",
                f"  {w_name} zayıflıyor → {s_name} daha güçlü görünüyor.",
                f"  {w_name}'den çıkıp {s_name} almayı değerlendirebilirsin.",
            ]
    else:
        lines.append("\nElinde açık coin pozisyonu görünmüyor.")

    lines.append("\n⚠️ Bu yatırım tavsiyesi değildir.")
    send_telegram("\n".join(lines))


# ---------------------------------------------------------------------------
# Telegram komut dinleyici (polling)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Tek coin analizi (doğal dil cevabı)
# ---------------------------------------------------------------------------

def analyze_single_coin(symbol: str, client: Client) -> None:
    """Bir coini analiz edip Telegram'a doğal dil cevabı gönderir."""
    send_telegram(f"🔍 {symbol} analiz ediliyor...")
    try:
        df = get_klines_df(client, symbol)
        if df is None:
            send_telegram(f"⚠️ {symbol} için veri alınamadı. Sembol doğru mu?")
            return

        ind = build_indicators(df)
        del df
        if ind is None:
            send_telegram(f"⚠️ {symbol} için indikatör hesaplanamadı.")
            return

        ticker_raw = client.get_ticker(symbol=symbol)
        btc_raw = client.get_ticker(symbol="BTCUSDT")
        btc_change = safe_float(btc_raw["priceChangePercent"])

        ticker = {
            "symbol": symbol,
            "lastPrice": str(ticker_raw["lastPrice"]),
            "priceChangePercent": str(ticker_raw["priceChangePercent"]),
            "quoteVolume": str(ticker_raw["quoteVolume"]),
        }
        coin = score_coin(ticker, btc_change, ind)
        if coin is None:
            send_telegram(f"⚠️ {symbol} skorlanamadı.")
            return

        rsi = ind["rsi"]
        ema_bull = ind["ema20"] > ind["ema50"] > 0
        macd_bull = ind["macd_hist"] > 0
        macd_cross = macd_bull and ind["macd_prev_hist"] <= 0
        score = coin["score"]
        coin_name = symbol.replace("USDT", "")

        # Fiyatın bant içindeki konumu
        bb_lower = ind["bb_lower"]
        bb_upper = ind["bb_upper"]
        close    = ind["close"]
        bb_pos_label = ""
        if bb_lower > 0 and bb_upper > 0:
            rng = bb_upper - bb_lower
            if rng > 0:
                bb_pos = (close - bb_lower) / rng
                if bb_pos < 0.2:
                    bb_pos_label = "Fiyat dip bölgesinde — alım için iyi seviye"
                elif bb_pos > 0.8:
                    bb_pos_label = "Fiyat zirveye yakın — dikkatli ol"

        # Genel karar
        if score >= 82:
            karar     = "✅ AL"
            aciklama  = "Sistemin tüm koşullarını geçti. Alım yapılabilir."
            tutar_str = f"💵 Önerilen tutar: {position_size(None, score)['amount'] if False else '—'} USDT\n   (Bakiyeni sonraki mesajda hesaplamak için /bakiye yaz)"
        elif score >= 75:
            karar    = "👀 BEKLE"
            aciklama = "İyi görünüyor ama henüz en yüksek güven eşiğine ulaşmadı. Biraz izle."
            tutar_str = ""
        elif score >= 60:
            karar    = "😐 NÖTR"
            aciklama = "Ne al ne sat. Piyasa net yön vermemiş."
            tutar_str = ""
        else:
            karar    = "❌ ALMA"
            aciklama = "Zayıf görünüm. Şu an bu coinden uzak dur."
            tutar_str = ""

        # Trend özeti — arka planda ne var onu sade anlat
        trend_parts = []
        if ema_bull:
            trend_parts.append("Fiyat yükseliş trendinde")
        else:
            trend_parts.append("Fiyat düşüş trendinde")
        if macd_cross:
            trend_parts.append("momentum yeni döndü")
        elif macd_bull:
            trend_parts.append("momentum güçlü")
        else:
            trend_parts.append("momentum zayıf")
        if rsi < 35:
            trend_parts.append("aşırı ucuz görünüyor")
        elif rsi > 70:
            trend_parts.append("aşırı pahalı görünüyor")

        lines = [
            f"🔍 {coin_name} ANALİZİ",
            "",
            f"💰 Güncel fiyat : {coin['price']:.6f} USDT",
            f"📊 Son 24 saat  : %{coin['change']:+.2f}",
            "",
            f"📝 Durum özeti  : {', '.join(trend_parts)}.",
        ]
        if bb_pos_label:
            lines.append(f"📌 {bb_pos_label}")

        lines += [
            "",
            f"━━━━━━━━━━━━━━━━━━",
            f"🤖 KARAR  :  {karar}",
            f"💬 {aciklama}",
        ]
        if tutar_str:
            lines.append(tutar_str)

        # Groq AI kısa yorum
        trending_list = getattr(market_intel, "trending", [])
        ai_comment = ai_coin_comment(
            symbol=symbol,
            score=score,
            price_change=coin.get("change", 0),
            trend="yukarı" if ema_bull else "aşağı",
            rsi=rsi,
            is_trending=symbol in trending_list,
        )
        if ai_comment:
            lines += ["", f"💬 AI Yorum: {ai_comment}"]

        send_telegram("\n".join(lines))

    except Exception as exc:
        logger.error("analyze_single_coin hata %s: %s", symbol, exc)
        send_telegram(f"⚠️ {symbol} analiz edilirken hata oluştu: {exc}")


def _extract_symbol(text: str) -> str | None:
    """Metinden coin sembolü çıkarmaya çalışır. 'sol', 'SOL', 'SOLUSDT' → 'SOLUSDT'"""
    words = text.upper().replace("?", "").replace("!", "").replace(",", "").split()
    for word in words:
        if word.endswith("USDT") and len(word) >= 6:
            return word
        candidate = word + "USDT"
        if 4 <= len(candidate) <= 12 and candidate.isalnum():
            return candidate
    return None


def handle_free_text(text: str, client: Client) -> None:
    """Komut olmayan serbest metin mesajlarını anlar ve cevap üretir."""
    text_lower = text.lower()

    # "Hepsini sat ve işlem yap" — elindeki coinleri USDT'ye çevirip seans başlat
    sell_all_kw  = ["hepsini sat", "her şeyi sat", "coinleri sat", "hepsi sat", "sat ve işlem"]
    if any(kw in text_lower for kw in sell_all_kw):
        parsed = _parse_session_command(text)
        end_time_str = ""
        if parsed:
            _, end_time = parsed
            end_time_str = end_time.strftime('%H:%M')

        send_telegram("⏳ Elindeki coinler USDT'ye çevriliyor...")
        try:
            balances = get_balances(client)
            result   = sell_all_to_usdt(client, balances)
            sold     = result["sold"]
            skipped  = result["skipped"]

            total_usdt = sum(s.get("proceeds", 0) for s in sold)
            lines = ["✅ Tüm coinler satıldı:"]
            for s in sold:
                lines.append(f"  • {s['symbol'].replace('USDT','')} → {s['proceeds']:.2f} USDT")
            if skipped:
                lines.append(f"  ⚠️ Satılamayan: {', '.join(skipped)}")
            lines.append(f"\n💵 Toplam USDT: ~{total_usdt:.2f}")
            send_telegram("\n".join(lines))

            # Seans başlat
            if parsed:
                _, end_time = parsed
                fresh_bal  = get_balances(client)
                fresh_usdt = get_usdt_balance(fresh_bal)
                sess = start_session(fresh_usdt, end_time)
                send_telegram(
                    f"🚀 SEANS BAŞLATILDI\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💵 Bütçe      : {fresh_usdt:.2f} USDT\n"
                    f"⏰ Bitiş saati: {end_time_str}\n\n"
                    f"En iyi fırsatlarda otomatik alım/satım yapılacak.\n"
                    f"Durdurmak için: /seansdurdur"
                )
            else:
                send_telegram(
                    "Seans başlatmak için saat de yaz:\n"
                    "Örnek: *hepsini sat 18:00*"
                )
        except Exception as exc:
            send_telegram(f"⚠️ Satış hatası: {exc}")
        return

    # Seans başlatma: "90 usdt 21:00" veya "150 dolar 09:30 a kadar"
    if any(kw in text_lower for kw in ["usdt", "dolar", "$"]) and re.search(r"\d{1,2}[:.]\d{2}", text):
        parsed = _parse_session_command(text)
        if parsed:
            budget, end_time = parsed
            if budget < 10:
                send_telegram("⚠️ Minimum seans bütçesi 10 USDT.")
                return
            sess = start_session(budget, end_time)
            mode = "🤖 OTOMATİK işlem yapacak" if AUTO_TRADE_ENABLED else "📢 Sinyal gönderecek (otomatik işlem kapalı)"
            send_telegram(
                f"✅ SEANS BAŞLATILDI\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 Bütçe      : {budget:.2f} USDT\n"
                f"⏰ Bitiş saati: {end_time.strftime('%H:%M')}\n"
                f"🤖 Mod        : {mode}\n\n"
                f"{'Bütçe bitene veya süre dolana kadar en iyi fırsatlarda otomatik alım/satım yapılacak.' if AUTO_TRADE_ENABLED else 'Bütçe dahilinde en iyi fırsatlar için sinyal gönderilecek.'}\n\n"
                f"Durdurmak için: /seansdurdur"
            )
            return

    # Coin analizi isteği tespiti
    symbol = _extract_symbol(text)
    analysis_keywords = ["nasıl", "ne durumda", "alsam", "almalı", "analiz", "bak", "incele", "nerede", "ne olur"]
    wants_analysis = symbol and any(kw in text_lower for kw in analysis_keywords)

    if wants_analysis or (symbol and len(text.split()) <= 3):
        analyze_single_coin(symbol, client)
        return

    # Genel soru tespiti
    if any(kw in text_lower for kw in ["ne alsam", "öner", "hangi coin", "fırsat", "tavsiye"]):
        _handle_portfolio(client)
        return

    if any(kw in text_lower for kw in ["portföy", "durumum", "coinlerim"]):
        _handle_portfolio(client)
        return

    if any(kw in text_lower for kw in ["bakiye", "param", "usdt", "para"]):
        try:
            balances = get_balances(client)
            usdt = get_usdt_balance(balances)
            send_telegram(f"💰 Spot USDT bakiyen: {usdt:.4f} USDT")
        except Exception as exc:
            send_telegram(f"⚠️ Bakiye alınamadı: {exc}")
        return

    # Analiz isteği serbest metin ile
    if any(kw in text_lower for kw in ["analiz", "istatistik", "kaç kazandım", "kazanma oranı", "geçmiş"]):
        state = load_state()
        trades = state.get("trades", [])
        if trades:
            stats = analyze_trade_history(trades[-100:])
            send_telegram(format_analysis_report(stats))
        else:
            send_telegram("📭 Henüz kayıtlı işlem yok.")
        return

    # Groq AI ile genel sorular — keyword'e takılmadan anlar
    ai_context = ""
    if not market_intel.is_stale():
        ai_context = market_intel.summary_text()
    fg = getattr(market_intel, "fear_greed", None)
    if fg:
        ai_context += f"\nFear&Greed: {fg['value']}/100"
    ai_context += f"\nDevre kesici: {circuit_breaker.state} ({circuit_breaker.reason})"

    ai_reply = ask_ai(text, ai_context)
    if ai_reply:
        send_telegram(f"🤖 {ai_reply}")
        return

    # AI yoksa (GROQ_API_KEY boş) eski keyword sistemi
    send_telegram(
        "🤔 Anlayamadım. Şöyle yazabilirsin:\n\n"
        "• *SOL nasıl?* — coin analizi\n"
        "• *Ne alsam?* — fırsat önerisi\n"
        "• *90 usdt 21:00* — seans başlat\n"
        "• */bakiye* — hesabını göster\n"
        "• */analiz* — işlem istatistikleri\n"
        "• */yardim* — tüm komutlar"
    )


COMMANDS_HELP = """📋 Kullanılabilir komutlar:

/portfolio    — Portföy analizi ve rotasyon önerileri
/bakiye       — Binance spot bakiyeni gösterir
/sinyaller    — Aktif sinyalleri listeler
/durum        — Bot durumu ve son tarama bilgisi
/analiz       — Son 100 işlemin istatistikleri
/seans        — Aktif seans bilgisi
/seansdurdur  — Seansı durdur, her şeyi USDT'ye çevir
/simulate     — Son 2 günlük simülasyon çalıştır
/backtest     — Gerçek geçmiş veri üzerinde backtest (varsayılan 30 gün, 20 sembol)
/backtest 90  — Son 90 günlük backtest
/yardim       — Bu listeyi gösterir

💡 Seans başlatmak için:
   *90 usdt 21:00*  yaz → 21:00'a kadar 90 USDT ile işlem yapar"""

_bot_state: dict = {"last_scan": None, "scan_count": 0, "last_cb_state": "NORMAL",
                    "btc_strength": {"strong": True, "trend_ok": True, "rsi_ok": True, "rsi": 50.0}}


def start_command_listener(client: Client) -> None:
    """Telegram mesajlarını ve callback query'leri polling ile dinler."""

    def _answer_callback(callback_id: str, text: str = "") -> None:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/answerCallbackQuery",
                json={"callback_query_id": callback_id, "text": text},
                timeout=10,
            )
        except Exception:
            pass

    def _reply(msg: str, markup: dict = None) -> None:
        """Komut yanıtları — dedup olmadan her zaman gönder."""
        send_telegram(msg, reply_markup=markup, dedup=False)

    def _handle_command(text: str, callback_id: str = None) -> None:
        if text == "/portfolio":
            logger.info("/portfolio komutu alındı.")
            if callback_id:
                _answer_callback(callback_id, "Analiz ediliyor...")
            _handle_portfolio(client)

        elif text == "/bakiye":
            try:
                balances = get_balances(client)
                usdt = get_usdt_balance(balances)
                eff = PORTFOLIO_SIZE_USDT or usdt
                lines = [
                    "💰 BAKİYE",
                    f"Spot USDT : {usdt:.4f} USDT",
                ]
                if PORTFOLIO_SIZE_USDT:
                    lines.append(f"Pozisyon  : {eff:.2f} USDT (manuel ayar)")
                assets = [(a, v) for a, v in balances.items() if a != "USDT" and v > 0]
                if assets:
                    lines.append("\nDiğer varlıklar:")
                    for asset, qty in assets:
                        lines.append(f"  {asset}: {qty:.6f}")
                _reply("\n".join(lines))
            except Exception as exc:
                _reply(f"⚠️ Bakiye hatası: {exc}")
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/sinyaller":
            state = load_state()
            active = {s: v for s, v in state["signals"].items() if v.get("status") == "active"}
            if not active:
                _reply("📭 Şu an aktif açık sinyal yok.")
            else:
                lines = [f"📡 AKTİF SİNYALLER ({len(active)} adet)\n"]
                for sym, sig in active.items():
                    lines.append(
                        f"• {sym}  Skor:{sig['score']}  Giriş:{sig['entry']:.6f}"
                        f"  Stop:{sig['stop']:.6f}  H1:{sig['target1']:.6f}"
                    )
                _reply("\n".join(lines))
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/durum":
            state = load_state()
            adaptive     = state.get("adaptive", {})
            active_count = sum(1 for s in state["signals"].values() if s.get("status") == "active")
            last         = _bot_state.get("last_scan")
            last_str     = last.strftime("%H:%M:%S") if last else "Henüz yok"
            sess         = get_active_session()
            sess_str     = (
                f"✅ Aktif ({sess.remaining_usdt:.0f} USDT kaldı, {sess.end_time.strftime('%H:%M')} bitiş)"
                if sess else "❌ Yok"
            )
            auto_str = "✅ AÇIK" if AUTO_TRADE_ENABLED else "❌ KAPALI (sinyal modu)"

            cb_emoji = circuit_breaker.status_emoji()
            cb_str   = f"{cb_emoji} {circuit_breaker.state}"
            if circuit_breaker.reason:
                cb_str += f" — {circuit_breaker.reason}"
            btc_15m  = btc_tracker.change_in_minutes(15)
            btc_15m_str = f"{btc_15m:+.2f}%" if btc_15m is not None else "veri yok"

            lines = [
                f"🤖 BOT DURUMU — {VERSION}",
                f"",
                f"Son tarama     : {last_str}",
                f"Toplam tarama  : {_bot_state['scan_count']}",
                f"Otomatik işlem : {auto_str}",
                f"Seans          : {sess_str}",
                f"",
                f"🛡 KORUMA SİSTEMİ",
                f"  Devre kesici : {cb_str}",
                f"  BTC 15 dk    : {btc_15m_str}",
                f"",
                f"📐 DİNAMİK PARAMETRELER",
                f"  Min skor : {adaptive.get('min_score', MIN_BUY_SCORE)}",
                f"  Stop %   : {adaptive.get('stop_pct', STOP_LOSS_PCT)*100:.1f}%",
            ]
            if not market_intel.is_stale():
                lines.append("")
                lines.append(market_intel.summary_text())
            _reply("\n".join(lines))
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/analiz":
            state = load_state()
            trades = state.get("trades", [])
            if not trades:
                _reply("📭 Henüz kaydedilmiş işlem yok.\nOtomatik işlem modu açık değil veya hiç işlem yapılmadı.")
            else:
                recent = trades[-100:]
                stats  = analyze_trade_history(recent)
                _reply(format_analysis_report(stats))
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/seans":
            sess = get_active_session()
            if sess:
                remaining = (sess.end_time - datetime.now(timezone.utc).replace(tzinfo=None)).seconds // 60
                _reply(
                    f"📋 AKTİF SEANS\n"
                    f"Bütçe      : {sess.budget_usdt:.2f} USDT\n"
                    f"Kalan      : {sess.remaining_usdt:.2f} USDT\n"
                    f"Bitiş      : {sess.end_time.strftime('%H:%M')}\n"
                    f"Kalan süre : ~{remaining} dakika\n"
                    f"İşlem sayısı: {len(sess.trades)}"
                )
            else:
                _reply(
                    "📭 Aktif seans yok.\n\n"
                    "Seans başlatmak için şöyle yaz:\n"
                    "  *90 usdt 21:00*\n"
                    "  *150 usdt 09:30*"
                )
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/seansdurdur":
            sess = get_active_session()
            if sess:
                _reply("⏹ Seans durduruluyor, pozisyonlar kapatılıyor...")
                _check_session_expiry(client, load_state())
                end_session()
                _reply("✅ Seans durduruldu.")
            else:
                _reply("Aktif seans yok.")
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/simulate":
            _reply("⏳ 2 günlük simülasyon başlatılıyor (3-5 dk sürebilir)...")
            if callback_id:
                _answer_callback(callback_id, "Simülasyon başlatıldı...")
            threading.Thread(
                target=_run_simulation_and_report,
                args=(client,),
                daemon=True,
            ).start()

        elif text.startswith("/backtest"):
            parts   = text.split()
            bt_days = 30
            bt_top  = 20
            for p in parts[1:]:
                if p.isdigit():
                    val = int(p)
                    if val <= 365:
                        bt_days = val
                    else:
                        bt_top = val
            _reply(
                f"⏳ Backtest başlatılıyor — {bt_days} günlük veri, {bt_top} sembol\n"
                f"Sonuç birkaç dakika içinde gelecek..."
            )
            if callback_id:
                _answer_callback(callback_id, "Backtest başlatıldı...")
            def _run_backtest():
                try:
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(
                        "backtest", os.path.join(os.path.dirname(__file__), "backtest.py")
                    )
                    bt = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(bt)

                    syms = bt.get_symbols(client, top_n=bt_top)
                    all_trades = []
                    interval   = Client.KLINE_INTERVAL_1HOUR
                    for sym in syms:
                        df = bt.fetch_ohlcv(client, sym, interval, bt_days)
                        if df is None or len(df) < 80:
                            continue
                        trades = bt.backtest_symbol(df, sym)
                        all_trades.extend(trades)
                    report = bt.build_report(all_trades, bt_days)
                    bt.save_json(all_trades)
                    for chunk in [report[i:i+3900] for i in range(0, len(report), 3900)]:
                        send_telegram(f"```\n{chunk}\n```", dedup=False)
                except Exception as e:
                    send_telegram(f"❌ Backtest hata: {e}", dedup=False)
            threading.Thread(target=_run_backtest, daemon=True).start()

        elif text in ("/yardim", "/help", "/start"):
            _reply(COMMANDS_HELP)
            if callback_id:
                _answer_callback(callback_id)

    def _poll():
        offset = 0
        logger.info("Telegram komut dinleyici başlatıldı.")
        while True:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
                resp = requests.get(
                    url,
                    params={"offset": offset, "timeout": 25},
                    timeout=30,
                )
                data = resp.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1

                    # Normal mesaj komutları
                    msg = update.get("message", {})
                    text = (msg.get("text") or "").strip()
                    if not text:
                        pass
                    elif text.startswith("/"):
                        _handle_command(text.split()[0])
                    else:
                        handle_free_text(text, client)

                    # Inline buton callback'leri (✅ Aldım / ❌ Atladım)
                    cb = update.get("callback_query", {})
                    if cb:
                        cb_data = cb.get("data", "")
                        cb_id = cb.get("id", "")
                        if cb_data.startswith("bought_"):
                            sym = cb_data.replace("bought_", "")
                            _answer_callback(cb_id, f"✅ {sym} alım kaydedildi!")
                            send_telegram(f"✅ {sym} için alım onayladın. Başarılar!")
                        elif cb_data.startswith("skip_"):
                            sym = cb_data.replace("skip_", "")
                            _answer_callback(cb_id, f"❌ {sym} atlandı.")

            except Exception as exc:
                logger.error("Telegram polling hatası: %s", exc)
                time.sleep(10)

    t = threading.Thread(target=_poll, daemon=True, name="telegram-listener")
    t.start()


def _handle_portfolio(client: Client) -> None:
    """Anında portföy raporu üretip Telegram'a gönderir."""
    try:
        send_telegram("⏳ Portföy analiz ediliyor...")
        state = load_state()
        balances = get_balances(client)
        tickers_map = get_tickers_map(client)

        btc = tickers_map.get("BTCUSDT")
        btc_change = safe_float(btc["priceChangePercent"]) if btc else 0.0

        candidates = prefilter_candidates(tickers_map, MIN_VOLUME_USDT)
        with _analysis_lock:
            scored = analyze_candidates(client, candidates, tickers_map, btc_change)

        portfolio_report(balances, scored, state)

        del tickers_map, candidates, scored, balances
        gc.collect()

    except Exception as exc:
        logger.exception("/portfolio hata: %s", exc)
        send_telegram(f"⚠️ Portföy raporu hatası:\n{exc}")


# ---------------------------------------------------------------------------
# Ana döngü
# ---------------------------------------------------------------------------

def _run_simulation_and_report(client: Client) -> None:
    """2 günlük 200 USDT simülasyonu çalıştırıp Telegram'a rapor gönderir."""
    try:
        from datetime import timezone as _tz
        import pandas as pd

        SIM_BALANCE   = 200.0
        SIM_DAYS      = 2
        SIM_MIN_SCORE = 82
        SIM_STOP      = 0.045
        SIM_T1        = 0.06
        SIM_T2        = 0.13
        SIM_TOP_N     = 15
        SIM_WARMUP    = 250
        SIM_ALLOC_80  = 0.15
        SIM_ALLOC_75  = 0.10
        SIM_MAX_POS   = 3

        now_tr      = datetime.now(_tz.utc).replace(tzinfo=None)
        total_hours = SIM_DAYS * 24
        total_cndl  = SIM_WARMUP + total_hours

        all_tickers = get_tickers_map(client)
        candidates  = prefilter_candidates(all_tickers, MIN_VOLUME_USDT)[:SIM_TOP_N]

        btc_raw = client.get_klines(symbol="BTCUSDT", interval="1h", limit=total_cndl)
        btc_close = [float(r[4]) for r in btc_raw]

        balance   = SIM_BALANCE
        positions = {}
        closed    = []
        signals   = []

        def _psize(bal, score):
            pct = SIM_ALLOC_80 if score >= 80 else SIM_ALLOC_75
            return round(bal * pct, 2)

        for sym in candidates:
            try:
                raw = client.get_klines(symbol=sym, interval="1h", limit=total_cndl)
                time.sleep(0.15)
            except Exception:
                continue
            if not raw or len(raw) < SIM_WARMUP + 2:
                continue

            df = pd.DataFrame(raw, columns=[
                "ot","open","high","low","close","volume",
                "ct","qv","t","tbb","tbq","i"
            ])
            for c in ("open","high","low","close","volume","qv"):
                df[c] = df[c].astype(float)
            df = df.rename(columns={"qv": "quote_volume"})

            for i in range(SIM_WARMUP, len(df)):
                hi = i - SIM_WARMUP
                if hi >= total_hours:
                    break
                price = float(df["close"].iloc[i])

                if sym in positions:
                    pos = positions[sym]
                    if price >= pos["t2"]:
                        pnl = (price - pos["entry"]) / pos["entry"] * pos["amount"]
                        balance += pos["amount"] + pnl
                        closed.append({"sym": sym, "res": "🎯H2", "pnl": pnl,
                                       "pct": (price-pos["entry"])/pos["entry"]*100})
                        del positions[sym]
                    elif price >= pos["t1"] and not pos.get("t1"):
                        pos["t1"] = True
                        half = pos["amount"] / 2
                        pnl  = (price - pos["entry"]) / pos["entry"] * half
                        balance += half + pnl
                        pos["amount"] -= half
                        pos["stop"]    = pos["entry"]
                    elif price <= pos["stop"]:
                        pnl = (price - pos["entry"]) / pos["entry"] * pos["amount"]
                        balance += pos["amount"] + pnl
                        closed.append({"sym": sym, "res": "🛑STOP", "pnl": pnl,
                                       "pct": (price-pos["entry"])/pos["entry"]*100})
                        del positions[sym]
                    continue

                if len(positions) >= SIM_MAX_POS:
                    continue

                df_sl = df.iloc[: i + 1][["open","high","low","close","volume","quote_volume"]].copy()
                ind   = build_indicators(df_sl)
                if ind is None:
                    continue

                btc_idx  = min(i, len(btc_close)-1)
                btc_prev = btc_close[max(0, btc_idx-24)]
                btc_chg  = ((btc_close[btc_idx]-btc_prev)/btc_prev*100) if btc_prev else 0
                if btc_chg < -2:
                    continue  # BTC düşerken sinyal üretme

                ch24 = 0.0
                if i >= 24:
                    p24 = float(df["close"].iloc[i-24])
                    ch24 = (price - p24) / p24 * 100 if p24 else 0

                fticker = {"symbol": sym, "lastPrice": str(price),
                           "priceChangePercent": str(round(ch24, 4)),
                           "quoteVolume": str(float(df["quote_volume"].iloc[max(0,i-23):i+1].sum()))}
                coin = score_coin(fticker, btc_chg, ind)
                if not coin or coin["score"] < SIM_MIN_SCORE:
                    continue

                amt = _psize(balance, coin["score"])
                if amt < 5 or amt > balance * 0.95:
                    continue

                balance -= amt
                positions[sym] = {
                    "entry": price, "amount": amt,
                    "stop": price*(1-SIM_STOP),
                    "t1":   price*(1+SIM_T1),
                    "t2":   price*(1+SIM_T2),
                }
                signals.append(f"  Saat {hi:02d}:00  {sym}  Skor:{coin['score']}  {coin.get('tier','')}  Giriş:{price:.4f}  Tutar:{amt:.0f}$")

        for sym, pos in list(positions.items()):
            try:
                lr = client.get_klines(symbol=sym, interval="1h", limit=2)
                lp = float(lr[-1][4])
            except Exception:
                lp = pos["entry"]
            pnl = (lp - pos["entry"]) / pos["entry"] * pos["amount"]
            balance += pos["amount"] + pnl
            closed.append({"sym": sym, "res": "⏰AÇIK", "pnl": pnl,
                           "pct": (lp-pos["entry"])/pos["entry"]*100})

        total_pnl = balance - SIM_BALANCE
        pct       = total_pnl / SIM_BALANCE * 100
        wins      = [c for c in closed if c["pnl"] > 0]
        wr        = len(wins)/len(closed)*100 if closed else 0
        emoji     = "🟢" if total_pnl >= 0 else "🔴"

        lines = [
            f"📊 2 GÜNLÜK SİMÜLASYON SONUCU",
            f"Başlangıç : 200.00 USDT",
            f"{emoji} Toplam PnL: {total_pnl:+.2f} USDT  [{pct:+.2f}%]",
            f"Sinyal sayısı : {len(signals)}",
            f"İşlem sayısı  : {len(closed)}",
            f"Kazanma oranı : %{wr:.0f}\n",
        ]
        if signals:
            lines.append("AL SİNYALLERİ:")
            lines += signals
        if closed:
            lines.append("\nKAPANAN İŞLEMLER:")
            for c in closed:
                lines.append(f"  {c['res']}  {c['sym']}  {c['pnl']:+.2f}$  ({c['pct']:+.1f}%)")
        lines.append("\n⚠️ Geçmiş performans gelecek sonuçları garantilemez.")
        send_telegram("\n".join(lines))

    except Exception as exc:
        logger.exception("Simülasyon hatası: %s", exc)
        send_telegram(f"⚠️ Simülasyon hatası: {exc}")


# ---------------------------------------------------------------------------
# Otomatik işlem yardımcıları
# ---------------------------------------------------------------------------

def _parse_session_command(text: str) -> Optional[tuple]:
    """
    '90 usdt 09:00' veya '150usdt 23:30 a kadar' gibi metni parse eder.
    (budget_usdt: float, end_time: datetime) döner veya None.
    """
    text_lower = text.lower()
    # Tutar: "90 usdt", "150usdt", "200$"
    m_amount = re.search(r"(\d+(?:\.\d+)?)\s*(?:usdt|\$|dolar)", text_lower)
    if not m_amount:
        return None
    budget = float(m_amount.group(1))

    # Saat: "09:00", "9.00", "21:30"
    m_time = re.search(r"(\d{1,2})[:\.](\d{2})", text)
    if not m_time:
        return None
    hour, minute = int(m_time.group(1)), int(m_time.group(2))

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    end_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if end_time <= now:
        end_time += timedelta(days=1)

    return budget, end_time


def _record_trade(state: dict, result: dict, signal: dict) -> None:
    """İşlemi zengin context ile kaydet — gerçek öğrenme için."""
    try:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        fg      = getattr(market_intel, "fear_greed", None)
        btc_str = _bot_state.get("btc_strength", {})
        entry   = {
            **result,
            "signal_score":   signal.get("score"),
            "stop":           signal.get("stop"),
            "target1":        signal.get("target1"),
            "target2":        signal.get("target2"),
            "atr_stop_pct":   signal.get("atr_stop_pct"),
            "exit_reason":    None,
            "pnl_usdt":       None,
            "pnl_pct":        None,
            # öğrenme context
            "weekday":        now_utc.strftime("%A"),
            "hour_utc":       now_utc.hour,
            "fear_greed":     fg,
            "btc_trend_ok":   btc_str.get("trend_ok"),
            "btc_rsi_ok":     btc_str.get("rsi_ok"),
            "btc_rsi":        btc_str.get("rsi"),
            "funding_rate":   (
                getattr(market_intel, "funding_rates", {}).get(signal.get("symbol", ""), None)
                if market_intel else None
            ),
            "reddit_score":   getattr(market_intel, "reddit_score", None),
        }
        state.setdefault("trades", []).append(entry)
    except Exception as e:
        logger.warning("_record_trade hata: %s", e)


def _auto_execute_signal(client: Client, signal: dict, state: dict) -> bool:
    """
    Sinyal için gerçek market alım yapar (seans varsa seans bütçesinden, yoksa bakiyeden).
    Başarılıysa True döner.
    """
    if not AUTO_TRADE_ENABLED:
        return False

    symbol = signal["symbol"]
    amount = signal["amount"]

    # Seans kontrolü
    sess = get_active_session()
    if sess:
        if not sess.can_trade(amount):
            logger.info("Seans bütçesi yetersiz, %s atlandı (%.2f USDT kaldı)", symbol, sess.remaining_usdt)
            return False
        result = market_buy(client, symbol, amount)
        if result:
            sess.record_buy(result, amount)
            state["signals"][symbol]["auto_qty"]   = result["qty"]
            state["signals"][symbol]["auto_price"]  = result["avg_price"]
            state["signals"][symbol]["auto_cost"]   = result["cost"]
            _apply_exit_levels(
                state["signals"][symbol],
                result["avg_price"],
                _effective_stop_pct(),
            )
            _record_trade(state, result, signal)
            send_telegram(
                f"🤖 OTOMATİK ALIM YAPILDI\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ {symbol.replace('USDT','')}  {result['avg_price']:.6f} fiyatından\n"
                f"💵 {result['cost']:.2f} USDT harcandı  |  {result['qty']:.6f} adet alındı\n"
                f"📋 Seans kalan bütçe: {sess.remaining_usdt:.2f} USDT"
            )
            return True
    else:
        # Seans yok ama AUTO_TRADE açık — direkt bakiyeden al
        result = market_buy(client, symbol, amount)
        if result:
            state["signals"][symbol]["auto_qty"]  = result["qty"]
            state["signals"][symbol]["auto_price"] = result["avg_price"]
            state["signals"][symbol]["auto_cost"]  = result["cost"]
            _apply_exit_levels(
                state["signals"][symbol],
                result["avg_price"],
                _effective_stop_pct(),
            )
            _record_trade(state, result, signal)
            send_telegram(
                f"🤖 OTOMATİK ALIM YAPILDI\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ {symbol.replace('USDT','')}  {result['avg_price']:.6f} fiyatından\n"
                f"💵 {result['cost']:.2f} USDT harcandı  |  {result['qty']:.6f} adet alındı"
            )
            return True
    return False


def _auto_execute_sell(client: Client, symbol: str, state: dict, reason: str) -> None:
    """Stop veya hedef tetiklenince otomatik satış yapar."""
    if not AUTO_TRADE_ENABLED:
        return
    sig = state["signals"].get(symbol)
    if not sig:
        return

    qty = sig.get("auto_qty", 0)
    if qty <= 0 and client:
        try:
            balances = get_balances(client)
            asset = symbol.replace("USDT", "")
            qty = balances.get(asset, 0)
            if qty > 0:
                sig["auto_qty"] = qty
        except Exception as exc:
            logger.error("auto_qty senkron hatası %s: %s", symbol, exc)

    if qty <= 0:
        logger.warning("Satış atlandı — miktar yok: %s", symbol)
        return

    result = market_sell(client, symbol, qty)
    if result:
        entry    = sig.get("auto_price", sig["entry"])
        pnl_usdt = result["proceeds"] - sig.get("auto_cost", entry * qty)

        # Geçmiş işlemi güncelle
        for t in reversed(state.get("trades", [])):
            if t.get("symbol") == symbol and t.get("exit_reason") is None:
                t["exit_reason"]  = reason
                t["pnl_usdt"]     = pnl_usdt
                t["exit_price"]   = result["avg_price"]
                t["exit_time"]    = result["time"]
                break

        emoji = "💰" if pnl_usdt >= 0 else "💸"
        send_telegram(
            f"🤖 OTOMATİK SATIŞ YAPILDI ({reason})\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{symbol.replace('USDT','')}  {result['avg_price']:.6f} fiyatından satıldı\n"
            f"{emoji} PnL: {pnl_usdt:+.2f} USDT"
        )
        sig["auto_qty"] = 0


def _check_session_expiry(client: Client, state: dict) -> None:
    """Seans süresi dolduysa tüm pozisyonları kapatır."""
    sess = get_active_session()
    if sess and sess.is_expired():
        send_telegram("⏰ Seans süresi doldu — tüm pozisyonlar USDT'ye çevriliyor...")
        try:
            balances = get_balances(client)
            result   = sell_all_to_usdt(client, balances)
            sold     = result["sold"]
            skipped  = result["skipped"]

            lines = [
                "✅ SEANS BİTTİ — TÜM POZİSYONLAR KAPATILDI",
                "",
                sess.summary(),
                "",
            ]
            if sold:
                lines.append("Satılan coinler:")
                for s in sold:
                    lines.append(f"  • {s['symbol'].replace('USDT','')}  {s['proceeds']:.2f} USDT")
            if skipped:
                lines.append(f"Satılamayan: {', '.join(skipped)} (manuel kontrol et)")

            send_telegram("\n".join(lines))
        except Exception as exc:
            logger.error("Seans kapanış hatası: %s", exc)
            send_telegram(f"⚠️ Seans kapanışında hata: {exc}")
        end_session()


def _run_nightly_learning(client: Client, state: dict) -> None:
    """Gece öğrenme döngüsünü çalıştırır ve parametreleri günceller."""
    from learner import run_nightly_learning
    trades = state.get("trades", [])
    adaptive = state.get("adaptive", {})
    cur_score = adaptive.get("min_score", MIN_BUY_SCORE)
    cur_stop  = adaptive.get("stop_pct",  STOP_LOSS_PCT)

    report = build_nightly_report(
        trades, client,
        current_min_score=cur_score,
        current_stop_pct=cur_stop,
    )
    send_telegram(report)

    # Parametreleri güncelle
    learning = run_nightly_learning(trades, cur_score, cur_stop)
    state["adaptive"]["min_score"] = learning["new_min_score"]
    state["adaptive"]["stop_pct"]  = learning["new_stop_pct"]
    state["last_nightly"] = datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d")
    save_state(state)


def run_bot() -> None:
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    auto_label = "🤖 OTOMATİK İŞLEM AÇIK" if AUTO_TRADE_ENABLED else "📢 Sinyal modu (işlem yok)"
    send_telegram(
        f"✅ Bihter Coin Signal {VERSION} başladı.\n"
        f"{auto_label}\n\n"
        + COMMANDS_HELP,
        dedup=False,
    )
    logger.info("Bot %s başlatıldı. AUTO_TRADE=%s", VERSION, AUTO_TRADE_ENABLED)

    start_command_listener(client)

    # İlk başlangıçta piyasa zekasını yükle (arka planda)
    threading.Thread(
        target=market_intel.refresh, args=(client,), daemon=True
    ).start()

    while True:
        try:
            state = load_state()

            # Seans süresi doldu mu?
            _check_session_expiry(client, state)

            # Gece öğrenme (her gece bir kez, UTC 21:00)
            now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
            today_str = now_utc.strftime("%Y-%m-%d")
            if (now_utc.hour == NIGHTLY_LEARN_HOUR_UTC
                    and state.get("last_nightly") != today_str
                    and len(state.get("trades", [])) > 0):
                threading.Thread(
                    target=_run_nightly_learning,
                    args=(client, state),
                    daemon=True,
                ).start()

            # Piyasa zekası 30 dakikada bir güncelle (arka planda)
            if market_intel.is_stale(max_age_minutes=30):
                threading.Thread(
                    target=market_intel.refresh, args=(client,), daemon=True
                ).start()

            # Dinamik parametreler (öğrenme motorundan gelen)
            adaptive      = state.get("adaptive", {})
            eff_min_score = adaptive.get("min_score", MIN_BUY_SCORE)
            eff_stop_pct  = adaptive.get("stop_pct",  STOP_LOSS_PCT)

            balances     = get_balances(client)
            usdt_balance = get_usdt_balance(balances)
            effective_usdt = PORTFOLIO_SIZE_USDT if PORTFOLIO_SIZE_USDT else usdt_balance

            # Seans aktifse kalan bütçeyi kullan
            sess = get_active_session()
            if sess:
                effective_usdt = min(effective_usdt, sess.remaining_usdt)

            tickers_map = get_tickers_map(client)
            btc = tickers_map.get("BTCUSDT")
            btc_change = safe_float(btc["priceChangePercent"]) if btc else 0.0

            # BTC fiyatını izleyiciye kaydet
            btc_price = safe_float(btc["lastPrice"]) if btc else 0.0
            btc_tracker.record(btc_price)

            # Güçlü BTC filtresi — her 3 taramada bir yenile (API tasarrufu)
            if _bot_state["scan_count"] % 3 == 0:
                btc_strength = get_btc_market_strength(client)
                _bot_state["btc_strength"] = btc_strength
            else:
                btc_strength = _bot_state.get("btc_strength", {"strong": True, "trend_ok": True, "rsi_ok": True})

            # Flash crash kontrolü
            btc_15m = btc_tracker.change_in_minutes(15)
            btc_5m  = btc_tracker.change_in_minutes(5)
            crash_msg = detect_flash_crash(btc_15m, btc_5m)
            if crash_msg:
                send_telegram(f"⚡ FLASH CRASH ALARMI\n{crash_msg}")

            # Portfolio drawdown hesapla
            drawdown = compute_portfolio_drawdown(state, tickers_map)

            # Devre kesici değerlendir
            cb_state = circuit_breaker.evaluate(btc_15m, btc_change, drawdown)
            alert    = circuit_breaker.alert_message()
            if alert and cb_state != _bot_state.get("last_cb_state"):
                send_telegram(alert)
                if cb_state == "CRITICAL" and AUTO_TRADE_ENABLED:
                    send_telegram("🔴 ACİL ÇIKIŞ: Tüm pozisyonlar kapatılıyor...")
                    result = sell_all_to_usdt(client, get_balances(client))
                    sold_names = [s["symbol"].replace("USDT","") for s in result["sold"]]
                    send_telegram(f"✅ Kapatılan: {', '.join(sold_names) if sold_names else 'Yok'}")
            _bot_state["last_cb_state"] = cb_state

            logger.info(
                "Tarama — BTC 24s: %.2f%%  15m: %s  CB: %s  DD: %.1f%%  USDT: %.2f",
                btc_change,
                f"{btc_15m:.1f}%" if btc_15m is not None else "?",
                cb_state, drawdown, usdt_balance,
            )

            candidates = prefilter_candidates(
                tickers_map,
                MIN_VOLUME_USDT,
                trending=getattr(market_intel, "trending", []) or [],
            )
            logger.info("%d momentum aday coin seçildi.", len(candidates))

            with _analysis_lock:
                scored = analyze_candidates(client, candidates, tickers_map, btc_change)

            if scored:
                logger.info("%d coin skorlandı. Top3: %s", len(scored),
                            [(c["symbol"], c["score"], f"{c['change']:.1f}%") for c in scored[:3]])

            # Bakiyedeki tüm coinleri takibe al, sonra satış kontrolü
            _sync_holdings_to_state(balances, state, tickers_map)
            check_active_signals(state, tickers_map, client, scored, eff_min_score)

            btc_weak = not btc_strength.get("strong", True)
            market_paused = (
                btc_change < BTC_PAUSE_THRESHOLD
                or not circuit_breaker.can_open_position()
                or btc_weak
            )
            if btc_weak:
                logger.info("BTC zayıf — EMA trend:%s RSI:%.1f, yeni alım kapalı.",
                            btc_strength.get("trend_ok"), btc_strength.get("rsi", 0))

            open_positions = _count_open_positions(state, balances, tickers_map)
            if open_positions >= MAX_OPEN_POSITIONS:
                logger.info("Max açık pozisyon (%d/%d), yeni alım kapalı.",
                            open_positions, MAX_OPEN_POSITIONS)
            if market_paused:
                logger.info("Piyasa durduruldu: BTC %.2f%%  CB:%s", btc_change, cb_state)

            # Pozisyon büyüklüğü çarpanı (devre kesici + volatilite)
            cb_mult = circuit_breaker.position_size_multiplier()

            new_signals: list = []
            if not market_paused and open_positions < MAX_OPEN_POSITIONS:
                for coin in scored:
                    # Piyasa zekası bonusunu ekle
                    intel_bonus = market_intel.market_bonus(
                        symbol=coin["symbol"],
                        price_change=coin.get("change", 0),
                    )
                    adjusted_score = coin["score"] + intel_bonus

                    if adjusted_score < eff_min_score:
                        continue
                    if should_send_new_signal(coin["symbol"], adjusted_score, state, balances):
                        coin["score"] = adjusted_score
                        signal = create_signal(coin, effective_usdt)
                        # ATR bazlı dinamik stop
                        try:
                            _df_atr = get_klines_df(client, coin["symbol"])
                            atr_stop = compute_atr_stop(_df_atr) if _df_atr is not None else _effective_stop_pct()
                            del _df_atr
                        except Exception:
                            atr_stop = _effective_stop_pct()
                        _apply_exit_levels(signal, signal["entry"], atr_stop)
                        signal["atr_stop_pct"] = round(atr_stop * 100, 2)
                        signal["intel_bonus"] = intel_bonus
                        # Volatilite ayarı
                        signal["amount"] = round(signal["amount"] * cb_mult, 2)

                        state["signals"][coin["symbol"]] = signal
                        new_signals.append(signal)
                    if len(new_signals) >= MAX_NEW_SIGNALS:
                        break

            if new_signals:
                send_buy_signals(new_signals, usdt_balance, effective_usdt)
                logger.info("%d yeni sinyal gönderildi.", len(new_signals))

                # Otomatik işlem
                if AUTO_TRADE_ENABLED:
                    for sig in new_signals:
                        _auto_execute_signal(client, sig, state)
                        time.sleep(0.5)

            now = time.time()
            if now - state.get("last_report", 0) >= REPORT_INTERVAL:
                portfolio_report(balances, scored, state)
                state["last_report"] = now

            save_state(state)
            _bot_state["last_scan"] = now_utc
            _bot_state["scan_count"] += 1

            del tickers_map, candidates, scored, balances, new_signals
            gc.collect()
            logger.info("Tarama tamamlandı. %ds sonraki tarama.", SCAN_INTERVAL)

        except BinanceAPIException as exc:
            if exc.status_code in (429, 418):
                wait = 65
                logger.warning("Rate limit, %ds bekleniyor.", wait)
                send_telegram(f"⚠️ Binance istek limiti — {wait}s bekleniyor...")
                time.sleep(wait)
            else:
                logger.exception("Binance API hatası: %s", exc)
                send_telegram(f"⚠️ {VERSION} Binance hatası:\n{exc}")
                gc.collect()
                time.sleep(SCAN_INTERVAL)

        except Exception as exc:
            logger.exception("Bot döngü hatası: %s", exc)
            send_telegram(f"⚠️ {VERSION} Bot hatası:\n{exc}")
            gc.collect()
            time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
