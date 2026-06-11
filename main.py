import gc
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta

import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
from dotenv import load_dotenv

from indicators import (
    analyze_candidates,
    prefilter_candidates,
    safe_float,
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

SCAN_INTERVAL = 300         # 5 dakika
REPORT_INTERVAL = 3600      # 1 saat
SIGNAL_COOLDOWN_HOURS = 2   # Aynı coin için minimum bekleme (skor ciddi artmadıkça)
SIGNAL_RESEND_SCORE_DELTA = 6  # Cooldown içinde yeniden göndermek için gereken skor artışı
MIN_VOLUME_USDT = 10_000_000
STATE_FILE = "state.json"

MAX_NEW_SIGNALS = 3
MIN_BUY_SCORE = 70          # 70+ takibe değer / 75+ güçlü / 80+ çok güçlü

STOP_LOSS_PCT = 0.03        # Stop zararı %3
TARGET1_PCT = 0.04          # Hedef 1 %4
TARGET2_PCT = 0.08          # Hedef 2 %8

MAX_CLOSED_SIGNALS = 50
MAX_SIGNAL_HISTORY = 100

VERSION = "V4"

# analyze_candidates eş zamanlı çalışmasını engeller (main loop + /portfolio)
_analysis_lock = threading.Semaphore(1)


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

def send_telegram(text: str, reply_markup: dict = None) -> None:
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("Telegram kimlik bilgileri eksik.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
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
            return json.load(fh)
    except Exception:
        return {"signals": {}, "last_report": 0, "closed": []}


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
    Risk-bazlı pozisyon hesabı.
    Stop %3 varsayılarak, portföye olan risk yüzdesi de gösterilir.

    Tier eşikleri:
        80+  → bakiyenin %15'i
        75+  → bakiyenin %10'u
        70+  → bakiyenin %5'i
    """
    if usdt_balance <= 0 or score < MIN_BUY_SCORE:
        return {"amount": 0.0, "alloc_pct": 0, "portfolio_risk_pct": 0.0}

    if score >= 80:
        alloc_pct = 0.15
    elif score >= 75:
        alloc_pct = 0.10
    else:
        alloc_pct = 0.05

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
        elapsed = datetime.utcnow() - last_time
        if elapsed < timedelta(hours=SIGNAL_COOLDOWN_HOURS):
            if score < existing["score"] + SIGNAL_RESEND_SCORE_DELTA:
                return False  # Cooldown içinde, skor yeterince artmadı
    return True


# ---------------------------------------------------------------------------
# Sinyal oluşturma
# ---------------------------------------------------------------------------

def create_signal(coin: dict, usdt_balance: float) -> dict:
    entry = coin["price"]
    pos = position_size(usdt_balance, coin["score"])
    return {
        "symbol": coin["symbol"],
        "time": datetime.utcnow().isoformat(),
        "entry": entry,
        "stop": round(entry * (1 - STOP_LOSS_PCT), 8),
        "target1": round(entry * (1 + TARGET1_PCT), 8),
        "target2": round(entry * (1 + TARGET2_PCT), 8),
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


def send_buy_signals(new_signals: list, usdt_balance: float) -> None:
    lines = [
        f"🚀 AL SİNYALİ — {VERSION}",
        f"💰 Spot USDT: {usdt_balance:.2f}\n",
    ]

    if usdt_balance == 0:
        lines.append(
            "⚠️ USDT Spot cüzdanda 0. Para Funding/Earn'deyse Spot'a transfer et.\n"
        )

    for idx, sig in enumerate(new_signals, 1):
        ema_arrow = "↑" if "YUKARI" in sig.get("ema_trend", "") else "↓"
        tier = sig.get("tier", "")
        lines += [
            f"{idx}) {sig['symbol']}  {tier}",
            f"   Skor    : {sig['score']}/100",
            f"   RSI     : {sig['rsi']}  |  EMA: {ema_arrow}  |  MACD: {sig['macd']}  |  Hacim: {sig['vol_spike']}x  |  ROC3: {sig['roc3']:+.1f}%",
            f"   Giriş   : {sig['entry']:.6f}",
            f"   Tutar   : {sig['amount']} USDT  (bakiyenin %{sig['alloc_pct']}'i | max risk: %{sig['portfolio_risk_pct']} portföy)",
            f"   Stop    : {sig['stop']:.6f}",
            f"   Hedef 1 : {sig['target1']:.6f}",
            f"   Hedef 2 : {sig['target2']:.6f}\n",
        ]

    lines.append("⚠️ Otomatik alım değildir. İşlem öncesi grafiği kontrol et.")

    buttons = []
    for sig in new_signals:
        buttons.append([
            {"text": f"✅ {sig['symbol']} Aldım", "callback_data": f"bought_{sig['symbol']}"},
            {"text": "❌ Atladım", "callback_data": f"skip_{sig['symbol']}"},
        ])

    send_telegram("\n".join(lines), reply_markup={"inline_keyboard": buttons})


# ---------------------------------------------------------------------------
# Aktif sinyal takibi
# ---------------------------------------------------------------------------

def check_active_signals(state: dict, tickers_map: dict) -> None:
    for symbol, signal in list(state["signals"].items()):
        if signal.get("status") not in ("active",):
            continue

        ticker = tickers_map.get(symbol)
        if not ticker:
            continue

        price = safe_float(ticker["lastPrice"])
        entry = signal["entry"]
        if entry <= 0:
            continue

        pnl = ((price - entry) / entry) * 100

        if price >= signal["target2"]:
            signal["status"] = "target2"
            send_telegram(f"🎯 HEDEF 2 GELDİ — {symbol}\nKâr: %{pnl:.2f}")

        elif price >= signal["target1"] and not signal.get("t1_sent"):
            signal["t1_sent"] = True
            send_telegram(
                f"✅ HEDEF 1 GELDİ — {symbol}\n"
                f"Kâr: %{pnl:.2f}\n"
                "Stop seviyeni girişe çekmeyi düşün. Kısmi kâr alınabilir."
            )

        elif price <= signal["stop"]:
            signal["status"] = "stopped"
            send_telegram(f"🛑 STOP TETİKLENDİ — {symbol}\nZarar: %{pnl:.2f}")


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


def portfolio_report(balances: dict, scored: list, state: dict = None) -> None:
    usdt = get_usdt_balance(balances)
    top3 = scored[:3]

    lines = [
        f"📊 PORTFÖY RAPORU — {VERSION}",
        f"💰 Spot USDT: {usdt:.2f}\n",
        "🔥 En güçlü fırsatlar:",
    ]

    for idx, coin in enumerate(top3, 1):
        lines.append(
            f"  {idx}) {coin['symbol']}  {coin['tier']}  Skor: {coin['score']}/100  "
            f"RSI: {coin['rsi']}  EMA: {coin['ema_trend']}  24s: %{coin['change']:.2f}"
        )

    # Elindeki coinleri değerlendir
    holdings = []
    for asset, amount in balances.items():
        if asset == "USDT":
            continue
        sym = asset + "USDT"
        match = next((c for c in scored if c["symbol"] == sym), None)
        if match:
            # Bilinen sinyal varsa PnL hesapla
            pnl_pct = None
            if state:
                sig = state.get("signals", {}).get(sym)
                if sig and sig.get("entry", 0) > 0:
                    current = match["price"]
                    pnl_pct = ((current - sig["entry"]) / sig["entry"]) * 100

            holdings.append({
                **match,
                "amount": amount,
                "pnl_pct": pnl_pct,
            })

    if holdings:
        holdings.sort(key=lambda x: x["score"], reverse=True)
        lines.append("\n📌 Elindeki coinler:")

        for h in holdings:
            action = _rotation_action(h["score"], h.get("pnl_pct"))
            pnl_str = f"  PnL: %{h['pnl_pct']:+.2f}" if h.get("pnl_pct") is not None else ""
            lines.append(
                f"  {action}  {h['symbol']}  {h['score']}/100  "
                f"RSI: {h['rsi']}  EMA: {h['ema_trend']}  24s: %{h['change']:.2f}{pnl_str}"
            )

        # Rotasyon önerisi: en zayıf coin ile en güçlü fırsat arasında büyük fark varsa
        weakest = holdings[-1]
        strongest = top3[0] if top3 else None
        if strongest and strongest["score"] - weakest["score"] >= 20:
            lines += [
                "\n🔄 ROTASYON ÖNERİSİ",
                f"  Zayıf  : {weakest['symbol']} ({weakest['score']}/100)",
                f"  Güçlü  : {strongest['symbol']} ({strongest['score']}/100  {strongest['tier']})",
                f"  Öneri  : {weakest['symbol']} pozisyonunun bir kısmını "
                f"{strongest['symbol']}'e taşımayı değerlendirebilirsin.",
            ]
    else:
        lines.append("\nAçık coin pozisyonu görünmüyor.")

    lines.append("\n⚠️ Bu yatırım tavsiyesi değildir.")
    send_telegram("\n".join(lines))


# ---------------------------------------------------------------------------
# /portfolio Telegram komutu (ayrı thread)
# ---------------------------------------------------------------------------

def start_command_listener(client: Client) -> None:
    """
    Telegram'dan gelen /portfolio komutunu polling ile dinler.
    Daemon thread olarak çalışır, ana döngüyü bloklamamaz.
    """

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
                    msg = update.get("message", {})
                    text = (msg.get("text") or "").strip()

                    if text == "/portfolio":
                        logger.info("/portfolio komutu alındı.")
                        _handle_portfolio(client)

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

def run_bot() -> None:
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    send_telegram(f"✅ Bihter Coin Signal {VERSION} başladı.\n"
                  f"📌 /portfolio yazarak istediğin zaman portföy raporu alabilirsin.")
    logger.info("Bot %s başlatıldı.", VERSION)

    start_command_listener(client)

    while True:
        try:
            state = load_state()

            balances = get_balances(client)
            usdt_balance = get_usdt_balance(balances)

            tickers_map = get_tickers_map(client)
            btc = tickers_map.get("BTCUSDT")
            btc_change = safe_float(btc["priceChangePercent"]) if btc else 0.0

            logger.info(
                "Tarama — BTC 24s: %.2f%%  USDT: %.2f",
                btc_change,
                usdt_balance,
            )

            # Birinci geçiş: ticker filtresi
            candidates = prefilter_candidates(tickers_map, MIN_VOLUME_USDT)
            logger.info("%d aday coin TA için seçildi.", len(candidates))

            # İkinci geçiş: kline + TA skorlama (semaphore ile /portfolio ile çakışma önlenir)
            with _analysis_lock:
                scored = analyze_candidates(client, candidates, tickers_map, btc_change)
            logger.info(
                "%d coin skorlandı. Top3: %s",
                len(scored),
                [(c["symbol"], c["score"]) for c in scored[:3]],
            )

            # Aktif sinyal güncelleme
            check_active_signals(state, tickers_map)

            # Yeni al sinyalleri
            new_signals: list = []
            for coin in scored:
                if coin["score"] < MIN_BUY_SCORE:
                    break
                if should_send_new_signal(coin["symbol"], coin["score"], state, balances):
                    signal = create_signal(coin, usdt_balance)
                    state["signals"][coin["symbol"]] = signal
                    new_signals.append(signal)
                if len(new_signals) >= MAX_NEW_SIGNALS:
                    break

            if new_signals:
                send_buy_signals(new_signals, usdt_balance)
                logger.info("%d yeni sinyal gönderildi.", len(new_signals))

            # Saatlik portföy raporu
            now = time.time()
            if now - state.get("last_report", 0) >= REPORT_INTERVAL:
                portfolio_report(balances, scored, state)
                state["last_report"] = now

            save_state(state)

            del tickers_map, candidates, scored, balances, new_signals
            gc.collect()

            logger.info("Tarama tamamlandı. %d sn sonraki tarama.", SCAN_INTERVAL)

        except BinanceAPIException as exc:
            if exc.status_code == 429 or exc.status_code == 418:
                wait = 65
                logger.warning("Binance rate limit aşıldı, %ds bekleniyor.", wait)
                send_telegram(f"⚠️ Binance istek limiti aşıldı. {wait} saniye bekleniyor...")
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
