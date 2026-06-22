"""
backtest.py  —  V6 Backtest Motoru
===================================
Son 1 yılın her saatlik mum verisi üzerinde sinyal/giriş/çıkış simüle eder.

Kullanım:
  python backtest.py                          # Tüm semboller, 30 gün
  python backtest.py --days 365 --top 50      # 50 sembol, 1 yıl
  python backtest.py --symbol BTCUSDT         # Tek sembol
  python backtest.py --days 90 --report html  # HTML rapor
"""

import os, sys, math, json, time, argparse, logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backtest")

# ─── Parametre yükle ──────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from binance.client import Client
from binance.exceptions import BinanceAPIException

API_KEY    = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Backtest parametreleri
INITIAL_BALANCE  = 200.0   # USDT
COMMISSION       = 0.001   # %0.1 her işlem
MIN_SCORE_BT     = 80      # Giriş için min skor
ATR_MULTIPLIER   = 2.0
PARTIAL_LEVELS   = [       # (pnl%, sat%)
    (0.06, 0.30),
    (0.10, 0.30),
    (0.15, 0.20),
]
TRAILING_PCT     = 0.03    # kalan %20 için trailing stop adımı

# ─── ATR ve skor hesaplama ────────────────────────────────────────────────────

def compute_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    delta = closes.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss.replace(0, float("nan"))
    return 100 - 100 / (1 + rs)


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def simple_score(df: pd.DataFrame) -> float:
    """Hızlı skor — 0-100 arası. Gerçek score_coin'in basitleştirilmiş hali."""
    try:
        if len(df) < 52:
            return 0
        c  = df["close"]
        v  = df["volume"]

        ema20 = c.ewm(span=20, adjust=False).mean()
        ema50 = c.ewm(span=50, adjust=False).mean()
        rsi   = compute_rsi(c)
        atr   = compute_atr(df)

        last_close = float(c.iloc[-1])
        last_ema20 = float(ema20.iloc[-1])
        last_ema50 = float(ema50.iloc[-1])
        last_rsi   = float(rsi.iloc[-1])
        avg_vol    = float(v.iloc[-20:].mean())
        last_vol   = float(v.iloc[-1])
        last_atr   = float(atr.iloc[-1])

        score = 0
        # EMA trend
        if last_ema20 > last_ema50:
            score += 20
        # RSI
        if 45 <= last_rsi <= 70:
            score += 20
        elif 35 <= last_rsi < 45 or 70 < last_rsi <= 80:
            score += 10
        # Volume spike
        if avg_vol > 0 and last_vol > avg_vol * 1.5:
            score += 15
        # Fiyat EMA üstünde
        if last_close > last_ema20:
            score += 15
        # Düşük ATR (volatilite uygun)
        atr_pct = last_atr / last_close if last_close > 0 else 0
        if 0.01 <= atr_pct <= 0.06:
            score += 15
        elif atr_pct < 0.01 or atr_pct > 0.10:
            score -= 10
        # Momentum (3 mum ROC)
        roc3 = (last_close - float(c.iloc[-4])) / float(c.iloc[-4]) * 100 if len(c) > 4 else 0
        if roc3 > 1:
            score += 15
        elif roc3 < -2:
            score -= 15

        return max(0, min(100, score))
    except Exception:
        return 0


def atr_stop(df: pd.DataFrame, multiplier: float = ATR_MULTIPLIER) -> float:
    """ATR bazlı stop yüzdesi."""
    try:
        atr_val   = float(compute_atr(df).iloc[-1])
        last_close = float(df["close"].iloc[-1])
        pct = (atr_val * multiplier) / last_close
        return max(0.015, min(0.15, pct))
    except Exception:
        return 0.05


# ─── Veri çekme ───────────────────────────────────────────────────────────────

def get_symbols(client: Client, top_n: int = 50) -> List[str]:
    """Hacme göre USDT paritelerini döner."""
    try:
        tickers = client.get_ticker()
        usdt = [t for t in tickers if t["symbol"].endswith("USDT") and float(t["quoteVolume"]) > 1_000_000]
        usdt.sort(key=lambda t: float(t["quoteVolume"]), reverse=True)
        exclude = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "USDTUSDT"}
        return [t["symbol"] for t in usdt if t["symbol"] not in exclude][:top_n]
    except Exception as e:
        logger.error("get_symbols: %s", e)
        return []


def fetch_ohlcv(client: Client, symbol: str, interval: str, days: int) -> Optional[pd.DataFrame]:
    """Geçmiş OHLCV çeker (1000 mum limiti aşılırsa parçalara böler)."""
    try:
        end_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = end_ms - days * 86_400_000

        all_klines = []
        cur_start  = start_ms
        while cur_start < end_ms:
            klines = client.get_klines(
                symbol   = symbol,
                interval = interval,
                startTime= cur_start,
                endTime  = end_ms,
                limit    = 1000,
            )
            if not klines:
                break
            all_klines.extend(klines)
            cur_start = klines[-1][0] + 1
            time.sleep(0.05)

        if not all_klines:
            return None

        df = pd.DataFrame(all_klines, columns=[
            "open_time","open","high","low","close","volume",
            "close_time","qv","trades","tbv","tqv","ignore"
        ])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        return df[["open","high","low","close","volume"]]
    except BinanceAPIException as e:
        logger.warning("fetch_ohlcv %s: %s", symbol, e)
        return None


# ─── Backtest motoru ──────────────────────────────────────────────────────────

class Trade:
    __slots__ = [
        "symbol","entry_time","entry_price","qty","cost","stop_pct","stop",
        "target2","partials","realized_pnl","status","exit_time","exit_price",
        "trailing_high","trailing_level",
    ]

    def __init__(self, symbol, entry_time, entry_price, alloc_usdt, stop_pct):
        self.symbol       = symbol
        self.entry_time   = entry_time
        self.entry_price  = entry_price
        self.cost         = alloc_usdt * (1 + COMMISSION)
        self.qty          = alloc_usdt / entry_price
        self.stop_pct     = stop_pct
        self.stop         = entry_price * (1 - stop_pct)
        self.target2      = entry_price * 1.25
        self.partials     = []       # (pct_threshold, sell_ratio, done)
        self.realized_pnl = 0.0
        self.status       = "open"
        self.exit_time    = None
        self.exit_price   = None
        self.trailing_high= entry_price
        self.trailing_level = 0

        # Kısmi kâr seviyeleri
        remaining_qty = self.qty
        for pct, ratio in PARTIAL_LEVELS:
            sell_qty = self.qty * ratio
            self.partials.append({
                "threshold": pct,
                "sell_qty":  sell_qty,
                "done":      False,
            })
            remaining_qty -= sell_qty
        self.partials.append({
            "threshold": None,   # trailing stop
            "sell_qty":  remaining_qty,
            "done":      False,
        })

    def update(self, high: float, low: float, close: float, dt: datetime) -> None:
        if self.status != "open":
            return

        # Kısmi kâr
        for p in self.partials:
            if p["done"] or p["threshold"] is None:
                continue
            if close >= self.entry_price * (1 + p["threshold"]):
                p["done"]        = True
                sell_revenue     = p["sell_qty"] * close * (1 - COMMISSION)
                sell_cost        = p["sell_qty"] * self.entry_price * (1 + COMMISSION)
                self.realized_pnl += sell_revenue - sell_cost

        # Trailing stop güncelle
        pnl_pct = (close - self.entry_price) / self.entry_price
        if pnl_pct >= 0.10 and self.trailing_level == 0:
            self.stop = max(self.stop, self.entry_price * 1.05)
            self.trailing_level = 1
        elif pnl_pct >= 0.05 and self.trailing_level < 1:
            self.stop = max(self.stop, self.entry_price)  # breakeven

        if close > self.trailing_high:
            self.trailing_high = close
        if self.trailing_level >= 1:
            trail_stop = self.trailing_high * (1 - TRAILING_PCT)
            self.stop  = max(self.stop, trail_stop)

        # Stop tetiklendi?
        if low <= self.stop:
            self._close(self.stop, dt, "stop")
        # Target2?
        elif high >= self.target2:
            self._close(self.target2, dt, "target2")

    def _close(self, price: float, dt: datetime, reason: str) -> None:
        self.status     = reason
        self.exit_price = price
        self.exit_time  = dt
        # Kalan açık kısımları kapat
        trailing_part   = self.partials[-1]
        remaining_qty   = sum(p["sell_qty"] for p in self.partials if not p["done"])
        if remaining_qty > 0:
            revenue           = remaining_qty * price * (1 - COMMISSION)
            cost_remain       = remaining_qty * self.entry_price * (1 + COMMISSION)
            self.realized_pnl += revenue - cost_remain

    @property
    def total_pnl(self) -> float:
        return self.realized_pnl

    @property
    def pnl_pct(self) -> float:
        return self.realized_pnl / (self.cost) * 100 if self.cost > 0 else 0


def backtest_symbol(
    df: pd.DataFrame,
    symbol: str,
    lookback: int = 60,   # skor hesaplamak için geriye bak
) -> List[Trade]:
    trades    = []
    in_trade  = False

    for i in range(lookback, len(df)):
        window = df.iloc[i - lookback: i]
        row    = df.iloc[i]
        dt     = df.index[i]
        close  = float(row["close"])
        high   = float(row["high"])
        low    = float(row["low"])

        if in_trade:
            trades[-1].update(high, low, close, dt)
            if trades[-1].status != "open":
                in_trade = False
            continue

        score = simple_score(window)
        if score < MIN_SCORE_BT:
            continue

        stop_pct = atr_stop(window)
        alloc    = INITIAL_BALANCE * _score_to_alloc(score)
        t        = Trade(symbol, dt, close, alloc, stop_pct)
        trades.append(t)
        in_trade = True

    return trades


def _score_to_alloc(score: float) -> float:
    if score >= 92:
        return 0.40
    elif score >= 89:
        return 0.30
    elif score >= 86:
        return 0.20
    else:
        return 0.15


# ─── Rapor ────────────────────────────────────────────────────────────────────

def build_report(all_trades: List[Trade], days: int) -> str:
    if not all_trades:
        return "Hiç işlem bulunamadı."

    closed = [t for t in all_trades if t.status != "open"]
    wins   = [t for t in closed if t.total_pnl > 0]
    losses = [t for t in closed if t.total_pnl <= 0]
    total_pnl  = sum(t.total_pnl for t in closed)
    win_rate   = len(wins) / len(closed) * 100 if closed else 0
    avg_win    = sum(t.pnl_pct for t in wins) / len(wins) if wins else 0
    avg_loss   = sum(t.pnl_pct for t in losses) / len(losses) if losses else 0
    pf         = abs(sum(t.total_pnl for t in wins)) / abs(sum(t.total_pnl for t in losses)) if losses and any(t.total_pnl < 0 for t in losses) else float("inf")

    by_status = {}
    for t in closed:
        by_status[t.status] = by_status.get(t.status, 0) + 1

    lines = [
        f"╔══════════════════════════════════════╗",
        f"║  BİHTER-COİN-SİGNAL  V6  BACKTEST   ║",
        f"╚══════════════════════════════════════╝",
        f"",
        f"📅 Dönem       : Son {days} gün",
        f"🔢 Toplam işlem: {len(closed)}  (açık: {len(all_trades)-len(closed)})",
        f"✅ Kazanan     : {len(wins)}   ({win_rate:.1f}%)",
        f"❌ Kaybeden    : {len(losses)}",
        f"",
        f"💰 Toplam PnL  : {total_pnl:+.2f} USDT",
        f"📈 Ort. kazanç : %{avg_win:.1f}",
        f"📉 Ort. kayıp  : %{avg_loss:.1f}",
        f"⚖️  Profit faktör: {pf:.2f}",
        f"",
        f"Çıkış nedenleri:",
    ]
    for reason, cnt in sorted(by_status.items(), key=lambda x: -x[1]):
        lines.append(f"  {reason:12s}: {cnt}")

    # En iyi / en kötü 5
    closed.sort(key=lambda t: t.pnl_pct, reverse=True)
    lines.append("\n🏆 En iyi 5 işlem:")
    for t in closed[:5]:
        dur = (t.exit_time - t.entry_time).total_seconds() / 3600 if t.exit_time else 0
        lines.append(
            f"  {t.symbol:15s}  {t.pnl_pct:+.1f}%  ({dur:.0f}h)  [{t.status}]"
        )
    lines.append("\n💣 En kötü 5 işlem:")
    for t in closed[-5:]:
        dur = (t.exit_time - t.entry_time).total_seconds() / 3600 if t.exit_time else 0
        lines.append(
            f"  {t.symbol:15s}  {t.pnl_pct:+.1f}%  ({dur:.0f}h)  [{t.status}]"
        )

    return "\n".join(lines)


def save_json(all_trades: List[Trade], path: str = "backtest_results.json") -> None:
    data = []
    for t in all_trades:
        data.append({
            "symbol":       t.symbol,
            "entry_time":   t.entry_time.isoformat() if hasattr(t.entry_time, "isoformat") else str(t.entry_time),
            "exit_time":    t.exit_time.isoformat()  if t.exit_time else None,
            "entry_price":  t.entry_price,
            "exit_price":   t.exit_price,
            "cost":         round(t.cost, 4),
            "pnl_usdt":     round(t.total_pnl, 4),
            "pnl_pct":      round(t.pnl_pct, 2),
            "status":       t.status,
            "stop_pct":     round(t.stop_pct * 100, 2),
        })
    with open(path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logger.info("JSON rapor kaydedildi: %s", path)


# ─── Ana giriş ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="V6 Backtest Motoru")
    parser.add_argument("--days",   type=int, default=30,   help="Kaç günlük veri (varsayılan: 30)")
    parser.add_argument("--top",    type=int, default=30,   help="Kaç sembol test edilecek")
    parser.add_argument("--symbol", type=str, default=None, help="Tek sembol testi (ör. SOLUSDT)")
    parser.add_argument("--json",   action="store_true",    help="Sonuçları JSON'a yaz")
    parser.add_argument("--min-score", type=int, default=MIN_SCORE_BT, help="Giriş için min skor")
    args = parser.parse_args()

    global MIN_SCORE_BT
    MIN_SCORE_BT = args.min_score

    if not API_KEY:
        logger.error("BINANCE_API_KEY bulunamadı — .env dosyasını kontrol et.")
        sys.exit(1)

    client = Client(API_KEY, API_SECRET)
    client.ping()
    logger.info("Binance bağlantısı OK")

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        symbols = get_symbols(client, top_n=args.top)
    logger.info("%d sembol test edilecek, %d günlük veri", len(symbols), args.days)

    all_trades: List[Trade] = []
    interval = Client.KLINE_INTERVAL_1HOUR

    for i, sym in enumerate(symbols):
        logger.info("[%d/%d] %s verisi çekiliyor...", i+1, len(symbols), sym)
        df = fetch_ohlcv(client, sym, interval, args.days)
        if df is None or len(df) < 80:
            logger.warning("%s: yetersiz veri (%s mum)", sym, len(df) if df is not None else 0)
            continue
        trades = backtest_symbol(df, sym)
        all_trades.extend(trades)
        logger.info("%s: %d işlem bulundu", sym, len(trades))
        time.sleep(0.1)

    print("\n" + build_report(all_trades, args.days))

    if args.json:
        save_json(all_trades)


if __name__ == "__main__":
    main()
