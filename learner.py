"""
learner.py — Piyasa zekası ve günlük öğrenme motoru  (V5)

Ücretsiz veri kaynakları:
  1. Fear & Greed Index        — alternative.me
  2. Reddit duygu analizi      — r/CryptoCurrency (auth gerektirmez)
  3. CryptoPanic haber skoru   — cryptopanic.com public API
  4. CoinGecko Trending        — coingecko.com free API
  5. Binance Open Interest      — Binance Futures API (ücretsiz)
  6. Büyük tasfiye (likidation) — Binance Futures agg trades üzerinden
  7. Günlük parametre öğrenmesi — kazanma/stop oranına göre eşik ayarı
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 1. Fear & Greed Index  (alternative.me — tamamen ücretsiz)
# ---------------------------------------------------------------------------

def get_fear_greed() -> Optional[dict]:
    try:
        resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        d = resp.json()["data"][0]
        return {"value": int(d["value"]), "label": d["value_classification"]}
    except Exception as exc:
        logger.warning("Fear&Greed hata: %s", exc)
        return None


def fear_greed_emoji(value: int) -> str:
    if value <= 25:  return "😱 Aşırı Korku"
    if value <= 45:  return "😨 Korku"
    if value <= 55:  return "😐 Nötr"
    if value <= 75:  return "😊 Açgözlülük"
    return "🤑 Aşırı Açgözlülük"


def fear_greed_signal(value: int) -> int:
    """Skor katkısı: aşırı korku = alım fırsatı (+5), aşırı açgözlülük = dikkat (−3)."""
    if value <= 20:   return +5
    if value <= 35:   return +2
    if value >= 80:   return -3
    if value >= 65:   return -1
    return 0


# ---------------------------------------------------------------------------
# 2. Reddit Duygu Analizi  (JSON API — auth gerektirmez)
# ---------------------------------------------------------------------------

_REDDIT_HEADERS = {"User-Agent": "bihter-coin-signal-bot/5.0"}
_REDDIT_SUBS    = ["CryptoCurrency", "CryptoMarkets", "altcoin"]


def get_reddit_sentiment(limit: int = 25) -> Optional[dict]:
    """
    r/CryptoCurrency, r/CryptoMarkets ve r/altcoin'den hot post başlıklarını çeker.
    Olumlu/olumsuz kelime sayısına göre duygu skoru üretir.
    """
    BULLISH_KW = [
        "bull", "moon", "pump", "breakout", "rally", "surge", "ath",
        "green", "buy", "long", "gains", "bullish", "up", "launch",
        "partnership", "adoption", "upgrade",
    ]
    BEARISH_KW = [
        "bear", "dump", "crash", "down", "sell", "short", "drop",
        "fear", "scam", "hack", "red", "bearish", "correction",
        "rug", "ban", "regulation", "lawsuit",
    ]
    scores    = []
    mentions: Dict[str, int] = {}

    for sub in _REDDIT_SUBS:
        try:
            url  = f"https://www.reddit.com/r/{sub}/hot.json?limit={limit}"
            resp = requests.get(url, headers=_REDDIT_HEADERS, timeout=10)
            posts = resp.json()["data"]["children"]
            for p in posts:
                title = p["data"].get("title", "").lower()
                bull  = sum(1 for kw in BULLISH_KW if kw in title)
                bear  = sum(1 for kw in BEARISH_KW if kw in title)
                scores.append(bull - bear)
                # Coin adı tespiti
                for word in title.split():
                    word = word.strip("$#.,!?").upper()
                    if 3 <= len(word) <= 6 and word.isalpha():
                        mentions[word] = mentions.get(word, 0) + 1
            time.sleep(0.5)
        except Exception as exc:
            logger.warning("Reddit %s hata: %s", sub, exc)

    if not scores:
        return None

    avg      = sum(scores) / len(scores)
    bullish  = sum(1 for s in scores if s > 0)
    bearish  = sum(1 for s in scores if s < 0)
    label    = "Olumlu" if avg > 0.3 else ("Olumsuz" if avg < -0.3 else "Karışık")

    # En çok bahsedilen coinler (genel kelimeler hariç)
    IGNORE = {"THE", "AND", "FOR", "NOT", "CAN", "ARE", "YOU", "HAS", "ITS",
              "ALL", "NEW", "TOP", "OUT", "GET", "USE", "HOW", "WHY", "NOW",
              "ANY", "BUT", "OUR", "TOO", "THIS", "THAT", "WITH", "FROM"}
    top = sorted(
        [(c, n) for c, n in mentions.items() if c not in IGNORE and n >= 2],
        key=lambda x: x[1], reverse=True
    )[:5]

    return {
        "score":   avg,
        "bullish": bullish,
        "bearish": bearish,
        "label":   label,
        "top_coins": top,
    }


def reddit_signal(score: float) -> int:
    """Reddit skoru → piyasa etkisi."""
    if score >= 1.0:   return +3
    if score >= 0.5:   return +1
    if score <= -1.0:  return -3
    if score <= -0.5:  return -1
    return 0


# ---------------------------------------------------------------------------
# 3. CryptoPanic Haber Duygusu  (public endpoint — ücretsiz)
# ---------------------------------------------------------------------------

def get_news_sentiment() -> Optional[dict]:
    try:
        resp  = requests.get(
            "https://cryptopanic.com/api/v1/posts/?auth_token=free&filter=hot&public=true",
            timeout=10,
        )
        posts = resp.json().get("results", [])[:10]
        if not posts:
            return None
        bull = sum(
            1 for p in posts
            if p.get("votes", {}).get("positive", 0) > p.get("votes", {}).get("negative", 0)
        )
        score = bull / len(posts) * 100
        return {
            "bullish": bull,
            "bearish": len(posts) - bull,
            "score":   score,
            "label":   "Olumlu" if score >= 60 else ("Olumsuz" if score <= 40 else "Karışık"),
        }
    except Exception as exc:
        logger.warning("CryptoPanic hata: %s", exc)
        return None


# ---------------------------------------------------------------------------
# 4. CoinGecko Trending Coins  (ücretsiz, auth yok)
# ---------------------------------------------------------------------------

def get_trending_coins() -> List[str]:
    """
    CoinGecko trending listesindeki coin sembollerini döner.
    ['SOLUSDT', 'SUIUSDT', ...]
    """
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/search/trending",
            timeout=10,
        )
        items = resp.json().get("coins", [])
        symbols = []
        for item in items[:7]:
            sym = item["item"].get("symbol", "").upper()
            if sym and not sym.endswith("USDT"):
                sym += "USDT"
            if sym:
                symbols.append(sym)
        return symbols
    except Exception as exc:
        logger.warning("CoinGecko trending hata: %s", exc)
        return []


def is_trending(symbol: str, trending: List[str]) -> bool:
    return symbol in trending


# ---------------------------------------------------------------------------
# 5. Binance Open Interest Değişimi  (ücretsiz Futures API)
# ---------------------------------------------------------------------------

def get_open_interest_changes(client, symbols: List[str]) -> Dict[str, float]:
    """
    Verilen futures sembollerinin 24 saatlik OI değişim yüzdelerini döner.
    {'BTCUSDT': +5.2, 'ETHUSDT': -1.3, ...}
    """
    result = {}
    for sym in symbols[:10]:
        try:
            data = client.futures_open_interest_hist(
                symbol=sym,
                period="1h",
                limit=25,
            )
            if not data or len(data) < 2:
                continue
            first = float(data[0]["sumOpenInterestValue"])
            last  = float(data[-1]["sumOpenInterestValue"])
            if first > 0:
                pct = (last - first) / first * 100
                result[sym] = round(pct, 2)
            time.sleep(0.1)
        except Exception:
            pass
    return result


def oi_signal(oi_change: float, price_change: float) -> int:
    """
    OI + fiyat kombinasyonu:
      OI artıyor + fiyat artıyor  = gerçek alım baskısı  (+3)
      OI artıyor + fiyat düşüyor  = short baskısı       (-2)
      OI azalıyor + fiyat düşüyor = pozisyon kapama      (0)
    """
    if oi_change > 5 and price_change > 0:   return +3
    if oi_change > 5 and price_change < 0:   return -2
    if oi_change < -5 and price_change < 0:  return 0
    if oi_change > 2 and price_change > 1:   return +1
    return 0


# ---------------------------------------------------------------------------
# 6. Binance Futures Fonlama Oranları  (ücretsiz)
# ---------------------------------------------------------------------------

def get_funding_rates(client, top_n: int = 5) -> list:
    try:
        rates  = client.futures_mark_price()
        parsed = []
        for r in rates:
            sym = r.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            fr = float(r.get("lastFundingRate", 0))
            parsed.append({"symbol": sym, "rate": fr})
        parsed.sort(key=lambda x: abs(x["rate"]), reverse=True)
        return parsed[:top_n]
    except Exception as exc:
        logger.warning("Fonlama hata: %s", exc)
        return []


def format_funding_rates(rates: list) -> str:
    if not rates:
        return "Fonlama verisi alınamadı."
    lines = ["📊 FONLAMA ORANLARI (Futures)"]
    for r in rates:
        pct   = r["rate"] * 100
        yorum = "⬇️ Düşüş baskısı" if pct > 0.05 else (
                "⬆️ Yükseliş baskısı" if pct < -0.05 else "➡️ Nötr")
        lines.append(f"  {r['symbol']:14s}  {pct:+.4f}%  {yorum}")
    return "\n".join(lines)


def funding_signal(rate: float) -> int:
    """
    Pozitif oran → long'lar ödüyor → aşırı long → ihtiyatlı.
    Negatif oran → short'lar ödüyor → potansiyel sıkışma/yükseliş.
    """
    if rate < -0.001:  return +2
    if rate < -0.0005: return +1
    if rate > 0.001:   return -1
    if rate > 0.002:   return -2
    return 0


# ---------------------------------------------------------------------------
# 7. Bütünleşik Piyasa Zekası Skoru
# ---------------------------------------------------------------------------

class MarketIntelligence:
    """
    Tüm ücretsiz veri kaynaklarından toplanan piyasa bilgisini saklar.
    Her 30 dakikada bir güncellenir.
    """
    def __init__(self):
        self.fear_greed: Optional[dict]    = None
        self.reddit:     Optional[dict]    = None
        self.news:       Optional[dict]    = None
        self.trending:   List[str]         = []
        self.funding:    List[dict]        = []
        self.oi_changes: Dict[str, float]  = {}
        self.last_update: Optional[datetime] = None
        self._lock = __import__("threading").Lock()
        # Kolay erişim özellikleri (öğrenme context için)
        self.funding_rates: Dict[str, float] = {}  # symbol → rate
        self.reddit_score:  Optional[float]  = None

    def is_stale(self, max_age_minutes: int = 30) -> bool:
        if not self.last_update:
            return True
        age = (datetime.now(timezone.utc).replace(tzinfo=None) - self.last_update).total_seconds() / 60
        return age > max_age_minutes

    def refresh(self, client=None) -> None:
        with self._lock:
            logger.info("Piyasa zekası güncelleniyor...")
            self.fear_greed = get_fear_greed()
            self.reddit     = get_reddit_sentiment()
            self.news       = get_news_sentiment()
            self.trending   = get_trending_coins()
            if client:
                try:
                    self.funding = get_funding_rates(client, top_n=10)
                    self.funding_rates = {f["symbol"]: f["rate"] for f in self.funding}
                except Exception:
                    pass  # Futures API erişimi yoksa sessizce geç
            if self.reddit:
                self.reddit_score = self.reddit.get("score")
            self.last_update = datetime.now(timezone.utc).replace(tzinfo=None)
            logger.info(
                "Piyasa zekası güncellendi — F&G:%s  Reddit:%s  Trend:%s",
                self.fear_greed.get("value") if self.fear_greed else "?",
                self.reddit.get("label") if self.reddit else "?",
                self.trending[:3],
            )

    def market_bonus(self, symbol: str = None, price_change: float = 0.0) -> int:
        """
        Belirli bir sembol veya genel piyasa için toplam bonus skor döner.
        Pozitif = alım yönünde, negatif = dikkatli ol.
        """
        bonus = 0

        if self.fear_greed:
            bonus += fear_greed_signal(self.fear_greed["value"])

        if self.reddit:
            bonus += reddit_signal(self.reddit["score"])

        if symbol and symbol in self.trending:
            bonus += 4   # trending coinde +4

        if symbol and self.funding:
            for f in self.funding:
                if f["symbol"] == symbol:
                    bonus += funding_signal(f["rate"])
                    break

        if symbol and symbol in self.oi_changes:
            bonus += oi_signal(self.oi_changes[symbol], price_change)

        return max(-10, min(10, bonus))   # -10 ile +10 arasında sınırla

    def summary_text(self) -> str:
        lines = ["🌐 PİYASA DURUMU"]

        if self.fear_greed:
            lines.append(
                f"  😱 Fear & Greed  : {self.fear_greed['value']}/100  "
                f"— {fear_greed_emoji(self.fear_greed['value'])}"
            )

        if self.reddit:
            top = ", ".join(f"{c}({n})" for c, n in self.reddit["top_coins"][:3])
            lines.append(
                f"  💬 Reddit duygusu: {self.reddit['label']}  "
                f"(Olumlu:{self.reddit['bullish']} Olumsuz:{self.reddit['bearish']})"
            )
            if top:
                lines.append(f"  🔥 Gündem coinler : {top}")

        if self.news:
            lines.append(
                f"  📰 Haber duygusu : {self.news['label']}  "
                f"(Olumlu:{self.news['bullish']} Olumsuz:{self.news['bearish']})"
            )

        if self.trending:
            lines.append(
                f"  📈 CoinGecko trend: {', '.join(s.replace('USDT','') for s in self.trending[:5])}"
            )

        return "\n".join(lines)


# Singleton — tüm uygulama aynı nesneyi kullanır
market_intel = MarketIntelligence()


# ---------------------------------------------------------------------------
# 8. Günlük Öğrenme: Parametre Otomatik Güncelleme
# ---------------------------------------------------------------------------

def _compute_adaptive_params(trades: list) -> dict:
    if len(trades) < 5:
        return {}
    recent   = trades[-20:]
    wins     = [t for t in recent if t.get("pnl_usdt", 0) > 0]
    stopped  = [t for t in recent if t.get("exit_reason") == "stop"]
    win_rate  = len(wins)    / len(recent)
    stop_rate = len(stopped) / len(recent)

    score_adj = +2 if win_rate < 0.40 else (-1 if win_rate > 0.70 else 0)
    stop_adj  = +0.005 if stop_rate > 0.50 else (-0.005 if stop_rate < 0.15 else 0.0)

    return {
        "score_adj": score_adj,
        "stop_adj":  stop_adj,
        "win_rate":  win_rate,
        "stop_rate": stop_rate,
    }


def run_nightly_learning(trades: list, current_min_score: int,
                          current_stop_pct: float) -> dict:
    params = _compute_adaptive_params(trades)
    new_min_score = current_min_score
    new_stop_pct  = current_stop_pct
    changes       = []

    adj = params.get("score_adj", 0)
    if adj != 0:
        new_min_score = max(78, min(92, current_min_score + adj))
        if new_min_score != current_min_score:
            changes.append(
                f"Min skor: {current_min_score} → {new_min_score}  "
                f"(kazanma %{params['win_rate']*100:.0f})"
            )

    sadj = params.get("stop_adj", 0.0)
    if sadj != 0.0:
        new_stop_pct = round(max(0.025, min(0.08, current_stop_pct + sadj)), 3)
        if new_stop_pct != current_stop_pct:
            changes.append(
                f"Stop: %{current_stop_pct*100:.1f} → %{new_stop_pct*100:.1f}  "
                f"(stop oranı %{params.get('stop_rate',0)*100:.0f})"
            )

    return {
        "new_min_score": new_min_score,
        "new_stop_pct":  new_stop_pct,
        "changes":       changes,
        "params":        params,
    }


# ---------------------------------------------------------------------------
# 9. Gece Raporu
# ---------------------------------------------------------------------------

def build_nightly_report(trades: list, client=None,
                          current_min_score: int = 82,
                          current_stop_pct: float = 0.045) -> str:
    from trader import analyze_trade_history, format_analysis_report

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lines = [
        f"🌙 GECE ANALİZ RAPORU — {now.strftime('%d.%m.%Y')}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # İşlem istatistikleri
    stats = analyze_trade_history(trades[-100:] if len(trades) > 100 else trades)
    lines.append(format_analysis_report(stats))
    lines.append("")

    # Parametre güncelleme
    learning = run_nightly_learning(trades, current_min_score, current_stop_pct)
    if learning["changes"]:
        lines.append("🔧 PARAMETRE GÜNCELLEMELERİ:")
        for ch in learning["changes"]:
            lines.append(f"  • {ch}")
        lines.append("")

    # Piyasa zekası özeti
    market_intel.refresh(client)
    lines.append(market_intel.summary_text())
    lines.append("")

    # Fonlama oranları
    if client:
        rates = get_funding_rates(client, top_n=5)
        if rates:
            lines.append(format_funding_rates(rates))
            lines.append("")

    lines.append("⚠️ Parametreler otomatik güncellendi.")
    return "\n".join(lines)
