import gc
import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from binance.client import Client
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

SCAN_INTERVAL = 300
REPORT_INTERVAL = 3600
SIGNAL_COOLDOWN_HOURS = 12
MIN_VOLUME_USDT = 10_000_000
STATE_FILE = "state.json"
MAX_NEW_SIGNALS = 3
MIN_BUY_SCORE = 82

# Kapalı sinyaller en fazla bu kadar tutulur (bellek sınırı)
MAX_CLOSED_SIGNALS = 50
# Aktif sinyal geçmişi en fazla bu kadar entry tutar
MAX_SIGNAL_HISTORY = 100

VERSION = "V4"


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
    """Kapalı sinyalleri ve eski girişleri silerek state büyümesini önler."""
    closed = state.get("closed", [])
    if len(closed) > MAX_CLOSED_SIGNALS:
        state["closed"] = closed[-MAX_CLOSED_SIGNALS:]

    signals = state.get("signals", {})
    if len(signals) > MAX_SIGNAL_HISTORY:
        sorted_items = sorted(
            signals.items(),
            key=lambda kv: kv[1].get("time", ""),
        )
        to_remove = len(signals) - MAX_SIGNAL_HISTORY
        for symbol, _ in sorted_items[:to_remove]:
            del signals[symbol]


# ---------------------------------------------------------------------------
# Binance veri çekme
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
# Sinyal mantığı
# ---------------------------------------------------------------------------

def should_send_new_signal(
    symbol: str, score: int, state: dict, balances: dict
) -> bool:
    asset = symbol.replace("USDT", "")
    if asset in balances and balances[asset] > 0:
        return False

    existing = state["signals"].get(symbol)
    if existing:
        last_time = datetime.fromisoformat(existing["time"])
        if datetime.utcnow() - last_time < timedelta(hours=SIGNAL_COOLDOWN_HOURS):
            if score < existing["score"] + 8:
                return False
    return True


def position_size(usdt_balance: float, score: int) -> float:
    if usdt_balance <= 0:
        return 0.0
    if score >= 95:
        pct = 0.20
    elif score >= 90:
        pct = 0.15
    elif score >= 82:
        pct = 0.10
    else:
        pct = 0.0
    return round(usdt_balance * pct, 2)


def create_signal(coin: dict, usdt_balance: float) -> dict:
    entry = coin["price"]
    return {
        "symbol": coin["symbol"],
        "time": datetime.utcnow().isoformat(),
        "entry": entry,
        "stop": round(entry * 0.97, 8),
        "target1": round(entry * 1.04, 8),
        "target2": round(entry * 1.08, 8),
        "score": coin["score"],
        "amount": position_size(usdt_balance, coin["score"]),
        "status": "active",
        "rsi": coin.get("rsi", 0),
        "ema_trend": coin.get("ema_trend", "-"),
        "macd": coin.get("macd", "-"),
        "vol_spike": coin.get("vol_spike", 1.0),
    }


def send_buy_signals(new_signals: list, usdt_balance: float) -> None:
    lines = [
        f"🚀 AKILLI AL SİNYALİ — {VERSION}\n",
        f"💰 Spot USDT: {usdt_balance:.2f}\n",
    ]

    if usdt_balance == 0:
        lines.append(
            "⚠️ USDT Spot cüzdanda 0 görünüyor. "
            "Para Funding/Earn tarafındaysa Spot'a transfer et.\n"
        )

    for idx, sig in enumerate(new_signals, 1):
        ema_arrow = "↑" if "YUKARI" in sig.get("ema_trend", "") else "↓"
        lines += [
            f"\n{idx}) {sig['symbol']}",
            f"   Skor     : {sig['score']}/100",
            f"   RSI      : {sig['rsi']}  |  EMA: {ema_arrow}  |  MACD: {sig['macd']}  |  Hacim: {sig['vol_spike']}x",
            f"   Giriş    : {sig['entry']:.6f}",
            f"   Tutar    : {sig['amount']} USDT",
            f"   Stop     : {sig['stop']:.6f}",
            f"   Hedef 1  : {sig['target1']:.6f}",
            f"   Hedef 2  : {sig['target2']:.6f}",
        ]

    lines.append("\n⚠️ Otomatik alım değildir. İşlem öncesi grafiği kontrol et.")

    # Her sinyal için ayrı inline buton satırı
    buttons = []
    for sig in new_signals:
        buttons.append([
            {"text": f"✅ {sig['symbol']} Aldım", "callback_data": f"bought_{sig['symbol']}"},
            {"text": f"❌ Atladım", "callback_data": f"skip_{sig['symbol']}"},
        ])

    reply_markup = {"inline_keyboard": buttons}
    send_telegram("\n".join(lines), reply_markup=reply_markup)


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
            send_telegram(
                f"🎯 HEDEF 2 GELDİ\n{symbol}\nKâr: %{pnl:.2f}"
            )
        elif price >= signal["target1"] and not signal.get("t1_sent"):
            signal["t1_sent"] = True
            send_telegram(
                f"✅ HEDEF 1 GELDİ\n{symbol}\nKâr: %{pnl:.2f}\n"
                "Kâr almayı değerlendirebilirsin."
            )
        elif price <= signal["stop"]:
            signal["status"] = "stopped"
            send_telegram(
                f"🛑 STOP SEVİYESİ\n{symbol}\nZarar: %{pnl:.2f}"
            )


# ---------------------------------------------------------------------------
# Portföy raporu
# ---------------------------------------------------------------------------

def portfolio_report(balances: dict, scored: list) -> None:
    usdt = get_usdt_balance(balances)
    top3 = scored[:3]

    lines = [
        f"📊 PORTFÖY RAPORU — {VERSION}\n",
        f"💰 Spot USDT: {usdt:.2f}\n",
        "🔥 En güçlü fırsatlar:",
    ]

    for idx, coin in enumerate(top3, 1):
        lines.append(
            f"  {idx}) {coin['symbol']}  Skor: {coin['score']}/100  "
            f"RSI: {coin['rsi']}  EMA: {coin['ema_trend']}  "
            f"24s: %{coin['change']:.2f}"
        )

    holdings = []
    for asset, amount in balances.items():
        if asset == "USDT":
            continue
        sym = asset + "USDT"
        match = next((c for c in scored if c["symbol"] == sym), None)
        if match:
            holdings.append({**match, "amount": amount})

    if holdings:
        holdings.sort(key=lambda x: x["score"], reverse=True)
        lines.append("\n📌 Elindeki coinler:")
        for h in holdings:
            if h["score"] >= 80:
                action = "TUT"
            elif h["score"] >= 65:
                action = "İZLE"
            else:
                action = "ZAYIF"
            lines.append(
                f"  - {h['symbol']}  {h['score']}/100  {action}  "
                f"RSI: {h['rsi']}  24s: %{h['change']:.2f}"
            )

        weakest = holdings[-1]
        strongest = top3[0] if top3 else None
        if strongest and strongest["score"] - weakest["score"] >= 20:
            lines += [
                "\n🔄 ROTASYON ÖNERİSİ",
                f"  Zayıf : {weakest['symbol']} ({weakest['score']}/100)",
                f"  Güçlü : {strongest['symbol']} ({strongest['score']}/100)",
                f"  Öneri : {weakest['symbol']} pozisyonunun bir kısmını "
                f"{strongest['symbol']}'e taşımayı değerlendirebilirsin.",
            ]
    else:
        lines.append("\nAçık coin pozisyonu görünmüyor.")

    lines.append("\n⚠️ Bu yatırım tavsiyesi değildir.")
    send_telegram("\n".join(lines))


# ---------------------------------------------------------------------------
# Ana döngü
# ---------------------------------------------------------------------------

def run_bot() -> None:
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    send_telegram(f"✅ Bihter Coin Signal {VERSION} başladı.")
    logger.info("Bot %s başlatıldı.", VERSION)

    while True:
        try:
            state = load_state()

            balances = get_balances(client)
            usdt_balance = get_usdt_balance(balances)

            tickers_map = get_tickers_map(client)

            btc = tickers_map.get("BTCUSDT")
            btc_change = safe_float(btc["priceChangePercent"]) if btc else 0.0

            logger.info(
                "Tarama başladı — BTC 24s: %.2f%%  USDT bakiye: %.2f",
                btc_change,
                usdt_balance,
            )

            # --- Birinci geçiş: hızlı ticker filtresi ---
            candidates = prefilter_candidates(tickers_map, MIN_VOLUME_USDT)
            logger.info("%d aday coin TA için seçildi.", len(candidates))

            # --- İkinci geçiş: kline + indikatör skorlaması ---
            scored = analyze_candidates(client, candidates, tickers_map, btc_change)
            logger.info("%d coin skorlandı. En yüksek: %s", len(scored), scored[:3] if scored else [])

            # --- Aktif sinyal güncelleme ---
            check_active_signals(state, tickers_map)

            # --- Yeni al sinyalleri ---
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

            # --- Saatlik rapor ---
            now = time.time()
            if now - state.get("last_report", 0) >= REPORT_INTERVAL:
                portfolio_report(balances, scored)
                state["last_report"] = now

            save_state(state)

            # --- Bellek temizliği ---
            del tickers_map, candidates, scored, balances, new_signals
            gc.collect()

            logger.info("Tarama tamamlandı. Sonraki tarama %d saniye sonra.", SCAN_INTERVAL)

        except Exception as exc:
            logger.exception("Bot döngü hatası: %s", exc)
            send_telegram(f"⚠️ {VERSION} Bot hatası:\n{exc}")
            gc.collect()

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
