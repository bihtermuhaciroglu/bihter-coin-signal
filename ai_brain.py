"""
ai_brain.py — Yapay Zeka Beyin ve Piyasa Koruma Sistemi  (V5)

Modüller:
  1. Groq AI (Llama 3.1 — ücretsiz)  : Telegram'da doğal dil anlama
  2. Flash Crash Dedektörü            : BTC 15 dk içinde %3+ düşüş
  3. Devre Kesici (Circuit Breaker)   : Aşırı düşüşte tüm işlemleri durdur
  4. Oynayanlık Filtresi (ATR-bazlı)  : Yüksek volatilitede pozisyon küçült
  5. Acil Çıkış Sistemi               : Portfolio %15 düşünce her şeyi sat
  6. BTC Fiyat Geçmişi İzleyici       : Son N mum fiyatlarını hafızada tutar
"""

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Groq AI — Llama 3.1 (ücretsiz, saniyede 30 istek)
# ---------------------------------------------------------------------------

GROQ_API_KEY  = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL    = "llama-3.1-8b-instant"   # ücretsiz, hızlı
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

_SYSTEM_PROMPT = """Sen "Bihter", Türkçe konuşan bir kripto para trading asistanısın.
Kullanıcıya kısa, net ve samimi cevaplar verirsin.
Teknik terimler yerine sade dil kullanırsın.
Her zaman şunu hatırlat: "Bu yatırım tavsiyesi değildir."

Bildiğin bağlam:
- Binance spot borsasında çalışıyorsun
- RSI, EMA, MACD gibi indikatörleri arka planda hesaplıyorsun ama kullanıcıya sadece net karar söylüyorsun
- Stop-loss %4.5, 1. hedef %6, 2. hedef %13
- Skor sistemi 0-100 arası, 82+ sinyal üretir
"""


def ask_ai(user_message: str, context: str = "") -> Optional[str]:
    """
    Groq AI'a soru sorar, Türkçe cevap döner.
    context: ek bağlam (piyasa durumu, portföy bilgisi vb.)
    """
    if not GROQ_API_KEY:
        return None

    messages = [{"role": "system", "content": _SYSTEM_PROMPT}]
    if context:
        messages.append({"role": "system", "content": f"Güncel bağlam:\n{context}"})
    messages.append({"role": "user", "content": user_message})

    try:
        resp = requests.post(
            GROQ_ENDPOINT,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":       GROQ_MODEL,
                "messages":    messages,
                "max_tokens":  400,
                "temperature": 0.4,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as exc:
        logger.warning("Groq AI hata: %s", exc)
        return None


def ai_coin_comment(symbol: str, score: int, price_change: float,
                    trend: str, rsi: float, is_trending: bool) -> str:
    """Bir coin için kısa AI yorumu üretir."""
    coin = symbol.replace("USDT", "")
    ctx  = (
        f"Coin: {coin}, Skor: {score}/100, 24s değişim: {price_change:+.1f}%, "
        f"Trend: {trend}, RSI: {rsi:.0f}, CoinGecko'da trend: {'Evet' if is_trending else 'Hayır'}"
    )
    prompt = f"{coin} için çok kısa (2 cümle) yorum yap. Almalı mıyım?"
    return ask_ai(prompt, ctx) or ""


# ---------------------------------------------------------------------------
# BTC Fiyat Geçmişi İzleyici
# ---------------------------------------------------------------------------

class BTCPriceTracker:
    """
    BTC spot fiyatını her taramada kaydeder.
    Son N fiyatı deque'da tutar — flash crash tespiti için kullanılır.
    """
    def __init__(self, maxlen: int = 20):
        self.prices: deque[tuple[datetime, float]] = deque(maxlen=maxlen)

    def record(self, price: float) -> None:
        self.prices.append((datetime.now(timezone.utc).replace(tzinfo=None), price))

    def change_in_minutes(self, minutes: int) -> Optional[float]:
        """Son N dakikadaki BTC değişim yüzdesini döner."""
        if len(self.prices) < 2:
            return None
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = now.replace(second=0, microsecond=0)
        # minutes öncesine en yakın fiyatı bul
        old_price = None
        for ts, px in self.prices:
            age_min = (now - ts).total_seconds() / 60
            if age_min <= minutes:
                old_price = px
                break
        if old_price is None:
            old_price = self.prices[0][1]
        current = self.prices[-1][1]
        if old_price <= 0:
            return None
        return (current - old_price) / old_price * 100

    def last_price(self) -> Optional[float]:
        return self.prices[-1][1] if self.prices else None


btc_tracker = BTCPriceTracker(maxlen=24)   # son 2 saatlik 5dk'lık taramalar


# ---------------------------------------------------------------------------
# Devre Kesici (Circuit Breaker)
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """
    Piyasa koşulları kötüleştiğinde işlemleri otomatik durdurur.

    Seviyeler:
      NORMAL    → Her şey yolunda
      CAUTION   → BTC 15 dk'da %-2 düştü       → Pozisyon boyutunu %50 küçült
      WARNING   → BTC 15 dk'da %-3 düştü       → Yeni işlem açma
      CRITICAL  → BTC 15 dk'da %-5 düştü       → Tüm pozisyonları kapat
    """

    NORMAL   = "NORMAL"
    CAUTION  = "CAUTION"
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"

    def __init__(self):
        self.state      = self.NORMAL
        self.triggered_at: Optional[datetime] = None
        self.reason     = ""

    def evaluate(self, btc_15m_change: Optional[float],
                 btc_1h_change: float,
                 portfolio_drawdown: float = 0.0) -> str:
        """
        Mevcut piyasa verilerine göre devre kesici durumunu günceller.
        Yeni durumu döner.
        """
        prev_state = self.state

        if btc_15m_change is not None and btc_15m_change <= -5.0:
            self._set(self.CRITICAL, f"BTC 15 dk'da {btc_15m_change:.1f}% düştü")
        elif btc_15m_change is not None and btc_15m_change <= -3.0:
            self._set(self.WARNING,  f"BTC 15 dk'da {btc_15m_change:.1f}% düştü")
        elif btc_15m_change is not None and btc_15m_change <= -2.0:
            self._set(self.CAUTION,  f"BTC 15 dk'da {btc_15m_change:.1f}% düştü")
        elif btc_1h_change <= -4.0:
            self._set(self.WARNING,  f"BTC 1 saatte {btc_1h_change:.1f}% düştü")
        elif portfolio_drawdown >= 15.0:
            self._set(self.CRITICAL, f"Portföy -%{portfolio_drawdown:.1f} düşüşte")
        elif portfolio_drawdown >= 8.0:
            self._set(self.WARNING,  f"Portföy -%{portfolio_drawdown:.1f} düşüşte")
        elif self.state != self.NORMAL:
            # Piyasa toparlandı mı? 30 dk sonra CAUTION/WARNING'i kaldır
            if self.triggered_at:
                age = (datetime.now(timezone.utc).replace(tzinfo=None) - self.triggered_at).total_seconds()
                if age > 1800 and btc_15m_change is not None and btc_15m_change > 0:
                    self._set(self.NORMAL, "Piyasa toparlandı")

        if self.state != prev_state:
            logger.warning("Devre kesici: %s → %s  (%s)", prev_state, self.state, self.reason)

        return self.state

    def _set(self, state: str, reason: str) -> None:
        self.state        = state
        self.reason       = reason
        self.triggered_at = datetime.now(timezone.utc).replace(tzinfo=None)

    def can_open_position(self) -> bool:
        return self.state in (self.NORMAL, self.CAUTION)

    def position_size_multiplier(self) -> float:
        """Pozisyon büyüklüğünü ne kadar küçülteceğimizi söyler."""
        return {
            self.NORMAL:   1.0,
            self.CAUTION:  0.5,
            self.WARNING:  0.0,
            self.CRITICAL: 0.0,
        }[self.state]

    def status_emoji(self) -> str:
        return {
            self.NORMAL:   "🟢",
            self.CAUTION:  "🟡",
            self.WARNING:  "🟠",
            self.CRITICAL: "🔴",
        }[self.state]

    def alert_message(self) -> str:
        emoji = self.status_emoji()
        return {
            self.CAUTION:  f"🟡 DİKKAT: {self.reason}\nPozisyon boyutu %50 küçültüldü.",
            self.WARNING:  f"🟠 UYARI: {self.reason}\nYeni işlem açılmıyor, mevcut pozisyonlar izleniyor.",
            self.CRITICAL: f"🔴 KRİTİK: {self.reason}\nACİL ÇIKIŞ başlatılıyor — tüm pozisyonlar kapatılıyor!",
        }.get(self.state, "")


# Singleton
circuit_breaker = CircuitBreaker()


# ---------------------------------------------------------------------------
# Oynayanlık (Volatility) Ölçer
# ---------------------------------------------------------------------------

def compute_atr_ratio(high_prices: list[float], low_prices: list[float],
                       close_prices: list[float], period: int = 14) -> float:
    """
    ATR / fiyat oranı döner — yüksekse piyasa çok oynak demek.
    > 0.03 (yüzde 3) → yüksek volatilite
    """
    if len(high_prices) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(high_prices)):
        tr = max(
            high_prices[i]  - low_prices[i],
            abs(high_prices[i] - close_prices[i-1]),
            abs(low_prices[i]  - close_prices[i-1]),
        )
        trs.append(tr)
    atr = sum(trs[-period:]) / period
    last_close = close_prices[-1]
    return atr / last_close if last_close > 0 else 0.0


def volatility_position_multiplier(atr_ratio: float) -> float:
    """
    Yüksek volatilitede daha küçük pozisyon aç.
    atr_ratio 0.03+ → %50 küçült
    atr_ratio 0.05+ → %70 küçült
    """
    if atr_ratio >= 0.05:  return 0.30
    if atr_ratio >= 0.04:  return 0.50
    if atr_ratio >= 0.03:  return 0.70
    return 1.0


# ---------------------------------------------------------------------------
# Flash Crash Dedektörü
# ---------------------------------------------------------------------------

def detect_flash_crash(btc_15m: Optional[float],
                        btc_5m:  Optional[float] = None) -> Optional[str]:
    """
    Flash crash varsa uyarı mesajı döner, yoksa None.
    """
    if btc_5m is not None and btc_5m <= -3.0:
        return f"⚡ FLASH CRASH TESPİT EDİLDİ!\nBTC son 5 dakikada {btc_5m:.1f}% düştü."
    if btc_15m is not None and btc_15m <= -4.0:
        return f"💥 SERT DÜŞÜŞ!\nBTC son 15 dakikada {btc_15m:.1f}% düştü."
    return None


# ---------------------------------------------------------------------------
# Portföy Drawdown Hesaplama
# ---------------------------------------------------------------------------

def compute_portfolio_drawdown(state: dict, tickers_map: dict) -> float:
    """
    Açık pozisyonların toplam drawdown yüzdesini döner.
    Tüm pozisyonlar kâr/zarardaysa ortalama düşüşü gösterir.
    """
    signals = state.get("signals", {})
    active  = [s for s in signals.values() if s.get("status") == "active"]
    if not active:
        return 0.0

    total_cost   = 0.0
    total_value  = 0.0
    for sig in active:
        sym   = sig["symbol"]
        entry = sig.get("entry", 0)
        amt   = sig.get("amount", 0)
        if entry <= 0 or amt <= 0:
            continue
        ticker = tickers_map.get(sym)
        if not ticker:
            continue
        from indicators import safe_float
        current = safe_float(ticker["lastPrice"])
        total_cost  += amt
        total_value += amt * (current / entry)

    if total_cost <= 0:
        return 0.0
    drawdown = (total_cost - total_value) / total_cost * 100
    return max(0.0, drawdown)
