import gc
import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

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

SCAN_INTERVAL = 300           # 5 dakika
REPORT_INTERVAL = 3600        # 1 saat
SIGNAL_COOLDOWN_HOURS = 6     # Aynı coin için minimum bekleme
SIGNAL_RESEND_SCORE_DELTA = 8 # Cooldown içinde yeniden göndermek için gereken skor artışı
MIN_VOLUME_USDT = 10_000_000
STATE_FILE = "state.json"

MAX_NEW_SIGNALS = 5            # Tek mesajda max sinyal sayısı
MIN_BUY_SCORE = 82             # Sadece gerçekten güçlü sinyaller

# Render Environment Variables'tan opsiyonel olarak set et:
# PORTFOLIO_SIZE_USDT=182  → gerçek bakiye düşükse bunu kullan
_env_portfolio = os.getenv("PORTFOLIO_SIZE_USDT")
PORTFOLIO_SIZE_USDT = float(_env_portfolio) if _env_portfolio else None

STOP_LOSS_PCT = 0.045          # Stop %4.5 — küçük dalgalanmada stop yemez
TARGET1_PCT = 0.06             # Hedef 1 %6
TARGET2_PCT = 0.13             # Hedef 2 %13

BTC_PAUSE_THRESHOLD = -2.0     # BTC bu kadar düşünce yeni sinyal gönderme
BTC_STRONG_MARKET = 1.5        # BTC bu kadar artınca piyasa sağlıklı

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
        elapsed = datetime.now(timezone.utc).replace(tzinfo=None) - last_time
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
        "time": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
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

        lines = [
            f"📢 AL SİNYALİ — {coin_name}",
            f"{strength}",
            "",
            f"💵 Ne kadar al?  →  {amount} USDT",
            f"📌 Alım fiyatı  →  {entry:.6f}",
            "",
            f"🛑 Zarar kes    →  {stop:.6f}  (−{risk_usd:.2f} USDT kaybedersin)",
            f"🎯 1. Hedef     →  {t1:.6f}  (+{gain1_usd:.2f} USDT kazanırsın)",
            f"🎯 2. Hedef     →  {t2:.6f}  (+{gain2_usd:.2f} USDT kazanırsın)",
            "",
            f"💡 Öneri: Alındıktan sonra stop-loss emirini borsa üzerinde {stop:.6f} fiyatına gir.",
            "",
            f"📊 Bakiyen: {usdt_balance:.2f} USDT  |  Bu işlem: %{sig['alloc_pct']} bakiyenden",
        ]

        buttons = [[
            {"text": f"✅ Aldım", "callback_data": f"bought_{sig['symbol']}"},
            {"text": "❌ Atladım", "callback_data": f"skip_{sig['symbol']}"},
        ]]
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

        now_str = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

        coin_name = symbol.replace("USDT", "")
        amount    = signal.get("amount", 0)
        pnl_usd   = round(amount * pnl / 100, 2)

        if price >= signal["target2"]:
            signal["status"] = "target2"
            signal["time"] = now_str
            send_telegram(
                f"🎯 {coin_name} — 2. HEDEFE ULAŞTI!\n\n"
                f"Tüm pozisyonu sat.\n"
                f"💰 Tahmini kâr: +{pnl_usd:.2f} USDT  (%{pnl:.1f})"
            )

        elif price >= signal["target1"] and not signal.get("t1_sent"):
            signal["t1_sent"] = True
            half_usd = round(pnl_usd / 2, 2)
            send_telegram(
                f"✅ {coin_name} — 1. HEDEFE ULAŞTI!\n\n"
                f"Yarısını sat, geri kalanı için stop-loss'u giriş fiyatına ({signal['entry']:.6f}) çek.\n"
                f"💰 Şu anki kâr: +{pnl_usd:.2f} USDT  (%{pnl:.1f})"
            )

        elif price <= signal["stop"]:
            signal["status"] = "stopped"
            signal["time"] = now_str
            send_telegram(
                f"🛑 {coin_name} — ZARAR KES TETİKLENDİ\n\n"
                f"Pozisyonu sat, zararı daha büyümeden çık.\n"
                f"💸 Tahmini zarar: {pnl_usd:.2f} USDT  (%{pnl:.1f})"
            )


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

    # Bilinmeyen mesaj
    send_telegram(
        "🤔 Anlayamadım. Şöyle yazabilirsin:\n\n"
        "• *SOL nasıl?* — coin analizi\n"
        "• *Ne alsam?* — fırsat önerisi\n"
        "• */bakiye* — hesabını göster\n"
        "• */sinyaller* — açık sinyaller\n"
        "• */yardim* — tüm komutlar"
    )


COMMANDS_HELP = """📋 Kullanılabilir komutlar:

/portfolio — Anlık portföy analizi ve rotasyon önerileri
/bakiye    — Binance spot bakiyeni gösterir
/sinyaller — Aktif açık sinyalleri listeler
/durum     — Bot durumu ve son tarama bilgisi
/simulate  — Son 2 günlük 200 USDT simülasyonu çalıştır
/yardim    — Bu listeyi gösterir"""

_bot_state: dict = {"last_scan": None, "scan_count": 0}


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
                send_telegram("\n".join(lines))
            except Exception as exc:
                send_telegram(f"⚠️ Bakiye hatası: {exc}")
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/sinyaller":
            state = load_state()
            active = {s: v for s, v in state["signals"].items() if v.get("status") == "active"}
            if not active:
                send_telegram("📭 Şu an aktif açık sinyal yok.")
            else:
                lines = [f"📡 AKTİF SİNYALLER ({len(active)} adet)\n"]
                for sym, sig in active.items():
                    lines.append(
                        f"• {sym}  Skor:{sig['score']}  Giriş:{sig['entry']:.6f}"
                        f"  Stop:{sig['stop']:.6f}  H1:{sig['target1']:.6f}"
                    )
                send_telegram("\n".join(lines))
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/durum":
            state = load_state()
            active_count = sum(1 for s in state["signals"].values() if s.get("status") == "active")
            last = _bot_state.get("last_scan")
            last_str = last.strftime("%H:%M:%S") if last else "Henüz yok"
            send_telegram(
                f"🤖 BOT DURUMU — {VERSION}\n\n"
                f"Son tarama   : {last_str}\n"
                f"Toplam tarama: {_bot_state['scan_count']}\n"
                f"Aktif sinyal : {active_count}\n"
                f"Tarama aralığı: {SCAN_INTERVAL}s\n"
                f"Min skor     : {MIN_BUY_SCORE}"
            )
            if callback_id:
                _answer_callback(callback_id)

        elif text == "/simulate":
            send_telegram("⏳ 2 günlük simülasyon başlatılıyor (3-5 dk sürebilir)...")
            if callback_id:
                _answer_callback(callback_id, "Simülasyon başlatıldı...")
            threading.Thread(
                target=_run_simulation_and_report,
                args=(client,),
                daemon=True,
            ).start()

        elif text in ("/yardim", "/help", "/start"):
            send_telegram(COMMANDS_HELP)
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


def run_bot() -> None:
    client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
    send_telegram(
        f"✅ Bihter Coin Signal {VERSION} başladı.\n\n"
        + COMMANDS_HELP
    )
    logger.info("Bot %s başlatıldı.", VERSION)

    start_command_listener(client)

    while True:
        try:
            state = load_state()

            balances = get_balances(client)
            usdt_balance = get_usdt_balance(balances)

            # Gerçek bakiye düşükse PORTFOLIO_SIZE_USDT env var'ı kullan
            effective_usdt = PORTFOLIO_SIZE_USDT if PORTFOLIO_SIZE_USDT else usdt_balance

            tickers_map = get_tickers_map(client)
            btc = tickers_map.get("BTCUSDT")
            btc_change = safe_float(btc["priceChangePercent"]) if btc else 0.0

            logger.info(
                "Tarama — BTC 24s: %.2f%%  Spot USDT: %.2f  Efektif: %.2f",
                btc_change, usdt_balance, effective_usdt,
            )

            # Birinci geçiş: ticker filtresi
            candidates = prefilter_candidates(tickers_map, MIN_VOLUME_USDT)
            logger.info("%d aday coin TA için seçildi.", len(candidates))

            # İkinci geçiş: kline + TA skorlama
            with _analysis_lock:
                scored = analyze_candidates(client, candidates, tickers_map, btc_change)

            if scored:
                logger.info(
                    "%d coin skorlandı. Top%d: %s",
                    len(scored), min(3, len(scored)),
                    [(c["symbol"], c["score"]) for c in scored[:3]],
                )

            # Aktif sinyal güncelleme
            check_active_signals(state, tickers_map)

            # BTC piyasa rejim filtresi — BTC düşerken yeni sinyal gönderme
            market_paused = btc_change < BTC_PAUSE_THRESHOLD
            if market_paused:
                logger.info("Piyasa durduruldu: BTC 24s %.2f%% (eşik: %.1f%%)", btc_change, BTC_PAUSE_THRESHOLD)

            # Yeni al sinyalleri (BTC düşerken üretme)
            new_signals: list = []
            if not market_paused:
                for coin in scored:
                    if coin["score"] < MIN_BUY_SCORE:
                        break
                    if should_send_new_signal(coin["symbol"], coin["score"], state, balances):
                        signal = create_signal(coin, effective_usdt)
                        state["signals"][coin["symbol"]] = signal
                        new_signals.append(signal)
                    if len(new_signals) >= MAX_NEW_SIGNALS:
                        break

            if new_signals:
                send_buy_signals(new_signals, usdt_balance, effective_usdt)
                logger.info("%d yeni sinyal gönderildi.", len(new_signals))

            # Saatlik portföy raporu
            now = time.time()
            if now - state.get("last_report", 0) >= REPORT_INTERVAL:
                portfolio_report(balances, scored, state)
                state["last_report"] = now

            save_state(state)

            _bot_state["last_scan"] = datetime.now(timezone.utc).replace(tzinfo=None)
            _bot_state["scan_count"] += 1

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
