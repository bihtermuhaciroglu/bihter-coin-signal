"""
learner.py — Günlük öğrenme ve piyasa zekası
- Her gece kazanan/kaybeden işlemleri analiz et
- MIN_BUY_SCORE ve STOP_LOSS_PCT otomatik ayarla
- Binance Futures fonlama oranları
- CryptoPanic haber duygu skoru
- Fear & Greed Index (Twitter/X yerine ücretsiz alternatif)
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fear & Greed Index (alternative.me — ücretsiz)
# ---------------------------------------------------------------------------

def get_fear_greed() -> Optional[dict]:
    """
    Kripto Fear & Greed Index çeker.
    {'value': int, 'label': str}  örn. {'value': 28, 'label': 'Fear'}
    """
    try:
        resp = requests.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=10,
        )
        data = resp.json()["data"][0]
        return {
            "value": int(data["value"]),
            "label": data["value_classification"],
        }
    except Exception as exc:
        logger.warning("Fear & Greed hatası: %s", exc)
        return None


def fear_greed_emoji(value: int) -> str:
    if value <= 25:
        return "😱 Aşırı Korku"
    if value <= 45:
        return "😨 Korku"
    if value <= 55:
        return "😐 Nötr"
    if value <= 75:
        return "😊 Açgözlülük"
    return "🤑 Aşırı Açgözlülük"


# ---------------------------------------------------------------------------
# CryptoPanic haber duygu (ücretsiz tier)
# ---------------------------------------------------------------------------

def get_news_sentiment() -> Optional[dict]:
    """
    CryptoPanic'ten son 10 haberin olumlu/olumsuz oranını döner.
    API key gerektirmiyor (public endpoint).
    """
    try:
        resp = requests.get(
            "https://cryptopanic.com/api/v1/posts/?auth_token=free&filter=hot&public=true",
            timeout=10,
        )
        posts = resp.json().get("results", [])[:10]
        if not posts:
            return None
        bullish = sum(1 for p in posts if p.get("votes", {}).get("positive", 0) >
                      p.get("votes", {}).get("negative", 0))
        bearish = len(posts) - bullish
        score   = bullish / len(posts) * 100
        return {
            "bullish": bullish,
            "bearish": bearish,
            "score":   score,
            "label":   "Olumlu" if score >= 60 else ("Olumsuz" if score <= 40 else "Karışık"),
        }
    except Exception as exc:
        logger.warning("Haber duygu hatası: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Binance Futures fonlama oranları (ücretsiz)
# ---------------------------------------------------------------------------

def get_funding_rates(client, top_n: int = 5) -> list:
    """
    En yüksek mutlak fonlama oranlı top_n coini döner.
    Pozitif oran → long'lar short'lara ödüyor → düşüş baskısı
    Negatif oran → short'lar long'lara ödüyor → yükseliş baskısı
    """
    try:
        rates = client.futures_mark_price()
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
        logger.warning("Fonlama oranı hatası: %s", exc)
        return []


def format_funding_rates(rates: list) -> str:
    if not rates:
        return "Fonlama verisi alınamadı."
    lines = ["📊 FONLAMA ORANLARI (Futures)"]
    for r in rates:
        rate_pct = r["rate"] * 100
        yorum    = "⬇️ Düşüş baskısı" if rate_pct > 0.05 else (
                   "⬆️ Yükseliş baskısı" if rate_pct < -0.05 else "➡️ Nötr")
        lines.append(f"  {r['symbol']:12s}  {rate_pct:+.4f}%  {yorum}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Günlük öğrenme: parametre otomatik güncelleme
# ---------------------------------------------------------------------------

def _compute_adaptive_params(trades: list) -> dict:
    """
    Son 20 işleme bakarak parametre önerileri üretir.
    """
    if len(trades) < 5:
        return {}

    recent = trades[-20:]
    wins   = [t for t in recent if t.get("pnl_usdt", 0) > 0]
    losses = [t for t in recent if t.get("pnl_usdt", 0) <= 0]
    win_rate = len(wins) / len(recent)

    # Kazanma oranı düşükse skoru artır, yüksekse biraz düşür
    if win_rate < 0.40:
        score_adj = +2   # daha seçici ol
    elif win_rate > 0.70:
        score_adj = -1   # biraz daha agresif olabilirsin
    else:
        score_adj = 0

    # Çok fazla stop yeniyorsa stop'u genişlet
    stopped = [t for t in recent if t.get("exit_reason") == "stop"]
    stop_rate = len(stopped) / len(recent)
    if stop_rate > 0.50:
        stop_adj = +0.005   # %0.5 genişlet
    elif stop_rate < 0.15:
        stop_adj = -0.005   # biraz daralt
    else:
        stop_adj = 0.0

    return {
        "score_adj": score_adj,
        "stop_adj":  stop_adj,
        "win_rate":  win_rate,
        "stop_rate": stop_rate,
    }


def run_nightly_learning(trades: list, current_min_score: int,
                          current_stop_pct: float) -> dict:
    """
    Gece çalışan öğrenme döngüsü.
    Güncellenmiş parametreleri ve raporu döner.
    """
    params = _compute_adaptive_params(trades)

    new_min_score  = current_min_score
    new_stop_pct   = current_stop_pct
    changes        = []

    if params.get("score_adj", 0) != 0:
        new_min_score = max(78, min(92, current_min_score + params["score_adj"]))
        if new_min_score != current_min_score:
            changes.append(
                f"Min skor: {current_min_score} → {new_min_score} "
                f"(kazanma oranı %{params['win_rate']*100:.0f})"
            )

    if params.get("stop_adj", 0.0) != 0.0:
        new_stop_pct = round(
            max(0.025, min(0.08, current_stop_pct + params["stop_adj"])), 3
        )
        if new_stop_pct != current_stop_pct:
            changes.append(
                f"Stop: %{current_stop_pct*100:.1f} → %{new_stop_pct*100:.1f} "
                f"(stop oranı %{params.get('stop_rate', 0)*100:.0f})"
            )

    return {
        "new_min_score": new_min_score,
        "new_stop_pct":  new_stop_pct,
        "changes":       changes,
        "params":        params,
    }


def build_nightly_report(trades: list, client=None,
                          current_min_score: int = 82,
                          current_stop_pct: float = 0.045) -> str:
    """
    Her gece çalışacak tam rapor metnini üretir.
    """
    from trader import analyze_trade_history, format_analysis_report

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    lines = [
        f"🌙 GECE ANALİZ RAPORU — {now.strftime('%d.%m.%Y')}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
    ]

    # 1) İşlem istatistikleri
    stats = analyze_trade_history(trades[-100:] if len(trades) > 100 else trades)
    lines.append(format_analysis_report(stats))
    lines.append("")

    # 2) Parametre güncelleme
    learning = run_nightly_learning(trades, current_min_score, current_stop_pct)
    if learning["changes"]:
        lines.append("🔧 PARAMETRE GÜNCELLEMELERİ:")
        for ch in learning["changes"]:
            lines.append(f"  • {ch}")
        lines.append("")

    # 3) Fear & Greed
    fg = get_fear_greed()
    if fg:
        lines.append(
            f"🧠 PIYASA DUYGÜSU\n"
            f"  Fear & Greed: {fg['value']}/100 — {fear_greed_emoji(fg['value'])}"
        )
        lines.append("")

    # 4) Haber duygu
    news = get_news_sentiment()
    if news:
        lines.append(
            f"📰 HABER DUYGUSU\n"
            f"  Olumlu: {news['bullish']}  Olumsuz: {news['bearish']}  "
            f"→ {news['label']}"
        )
        lines.append("")

    # 5) Fonlama oranları
    if client:
        rates = get_funding_rates(client, top_n=5)
        if rates:
            lines.append(format_funding_rates(rates))
            lines.append("")

    lines.append("⚠️ Parametreler otomatik güncellendi.")
    return "\n".join(lines)
