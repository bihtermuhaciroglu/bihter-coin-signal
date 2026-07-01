"""
trader.py — Otomatik işlem motoru
- Binance VE Bybit emir gönderme (market buy/sell)
- Lot size / min notional / step size kontrolü
- Seans yönetimi (bütçe + bitiş saati)
- İşlem geçmişi ve analiz
"""

import logging
import math
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict

try:
    from binance.client import Client as BinanceClient
    from binance.exceptions import BinanceAPIException
except ImportError:
    BinanceClient = None
    BinanceAPIException = Exception

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Binance sembol kuralları
# ---------------------------------------------------------------------------

_symbol_info_cache: dict = {}


def _is_bybit(client) -> bool:
    return type(client).__name__ == "BybitClient"


def get_symbol_rules(client, symbol: str) -> Optional[dict]:
    """LOT_SIZE ve MIN_NOTIONAL filtrelerini döner, cache'ler."""
    if symbol in _symbol_info_cache:
        return _symbol_info_cache[symbol]
    try:
        if _is_bybit(client):
            info = client.get_exchange_info()
            for s in info.get("symbols", []):
                if s["symbol"] == symbol:
                    rules = {"min_qty": 0.00001, "step_size": 0.00001, "min_notional": 1.0}
                    for f in s.get("filters", []):
                        if f["filterType"] == "LOT_SIZE":
                            rules["min_qty"]   = float(f.get("minQty", 0.00001))
                            rules["step_size"] = float(f.get("stepSize", 0.00001))
                        elif f["filterType"] == "MIN_NOTIONAL":
                            rules["min_notional"] = float(f.get("minNotional", 1.0))
                    _symbol_info_cache[symbol] = rules
                    return rules
            return {"min_qty": 0.00001, "step_size": 0.00001, "min_notional": 1.0}
        else:
            info = client.get_symbol_info(symbol)
            if not info:
                return None
            rules = {"min_qty": 0.0, "step_size": 0.0, "min_notional": 10.0}
            for f in info.get("filters", []):
                if f["filterType"] == "LOT_SIZE":
                    rules["min_qty"]    = float(f["minQty"])
                    rules["step_size"]  = float(f["stepSize"])
                elif f["filterType"] == "NOTIONAL":
                    rules["min_notional"] = float(f.get("minNotional", 10))
                elif f["filterType"] == "MIN_NOTIONAL":
                    rules["min_notional"] = float(f.get("minNotional", 10))
            _symbol_info_cache[symbol] = rules
            return rules
    except Exception as exc:
        logger.error("get_symbol_rules %s: %s", symbol, exc)
        return None


def _round_step(qty: float, step_size: float) -> float:
    """Miktarı Binance lot step size'a göre yuvarlar."""
    if step_size <= 0:
        return qty
    precision = int(round(-math.log10(step_size)))
    return math.floor(qty / step_size) * step_size


def _qty_precision(step_size: float) -> int:
    if step_size <= 0:
        return 8
    return max(0, int(round(-math.log10(step_size))))


# ---------------------------------------------------------------------------
# Market buy / sell
# ---------------------------------------------------------------------------

def market_buy(client: Client, symbol: str, usdt_amount: float) -> Optional[dict]:
    """
    USDT miktarı kadar market alım yapar.
    Başarılıysa {'symbol', 'qty', 'avg_price', 'cost'} döner.
    """
    rules = get_symbol_rules(client, symbol)
    if not rules:
        logger.error("market_buy: %s için kural alınamadı", symbol)
        return None

    if usdt_amount < rules["min_notional"]:
        logger.warning("market_buy: %s için tutar çok düşük (%.2f < %.2f)",
                        symbol, usdt_amount, rules["min_notional"])
        return None

    try:
        order = client.order_market_buy(
            symbol=symbol,
            quoteOrderQty=round(usdt_amount, 2),
        )
        # Binance fills listesi döner, Bybit farklı format
        fills = order.get("fills", [])
        if fills:
            total_qty  = sum(float(f["qty"]) for f in fills)
            total_cost = sum(float(f["qty"]) * float(f["price"]) for f in fills)
            avg_price  = total_cost / total_qty if total_qty else 0
        else:
            # Bybit — anlık fiyattan tahmin et
            avg_price  = float(order.get("avgPrice") or order.get("price") or 0)
            total_qty  = float(order.get("cumExecQty") or order.get("qty") or 0)
            total_cost = float(order.get("cumExecValue") or (total_qty * avg_price))
            if avg_price == 0 and total_cost > 0 and total_qty > 0:
                avg_price = total_cost / total_qty

        result = {
            "symbol":    symbol,
            "qty":       total_qty,
            "avg_price": avg_price,
            "cost":      total_cost,
            "order_id":  order.get("orderId"),
            "time":      datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            "side":      "BUY",
        }
        logger.info("BUY  %s  qty=%.6f  avg=%.6f  cost=%.2f USDT",
                    symbol, total_qty, avg_price, total_cost)
        return result

    except Exception as exc:
        logger.error("market_buy hata %s: %s", symbol, exc)
        return None


def market_sell(client: Client, symbol: str, qty: float) -> Optional[dict]:
    """
    Belirli miktarda market satış yapar.
    Başarılıysa {'symbol', 'qty', 'avg_price', 'proceeds'} döner.
    """
    rules = get_symbol_rules(client, symbol)
    if not rules:
        logger.error("market_sell: %s için kural alınamadı", symbol)
        return None

    qty_rounded = _round_step(qty, rules["step_size"])
    if qty_rounded < rules["min_qty"]:
        logger.warning("market_sell: %s qty çok küçük (%.8f)", symbol, qty_rounded)
        return None

    try:
        precision = _qty_precision(rules["step_size"])
        order = client.order_market_sell(
            symbol=symbol,
            quantity=f"{qty_rounded:.{precision}f}",
        )
        fills    = order.get("fills", [])
        if fills:
            total_qty = sum(float(f["qty"]) for f in fills)
            total_rev = sum(float(f["qty"]) * float(f["price"]) for f in fills)
            avg_price = total_rev / total_qty if total_qty else 0
        else:
            avg_price = float(order.get("avgPrice") or order.get("price") or 0)
            total_qty = float(order.get("cumExecQty") or order.get("qty") or qty_rounded)
            total_rev = float(order.get("cumExecValue") or (total_qty * avg_price))

        result = {
            "symbol":    symbol,
            "qty":       total_qty,
            "avg_price": avg_price,
            "proceeds":  total_rev,
            "order_id":  order.get("orderId"),
            "time":      datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
            "side":      "SELL",
        }
        logger.info("SELL %s  qty=%.6f  avg=%.6f  proceeds=%.2f USDT",
                    symbol, total_qty, avg_price, total_rev)
        return result

    except Exception as exc:
        logger.error("market_sell hata %s: %s", symbol, exc)
        return None


def sell_all_to_usdt(client: Client, balances: dict) -> dict:
    """
    Tüm USDT-dışı varlıkları USDT'ye çevirir.
    {'sold': [...], 'skipped': [...]} döner.
    """
    sold    = []
    skipped = []
    for asset, qty in balances.items():
        if asset == "USDT":
            continue
        symbol = asset + "USDT"
        res = market_sell(client, symbol, qty)
        if res:
            sold.append(res)
        else:
            skipped.append(asset)
        time.sleep(0.3)
    return {"sold": sold, "skipped": skipped}


# ---------------------------------------------------------------------------
# Seans yönetimi
# ---------------------------------------------------------------------------

class TradingSession:
    """
    Kullanıcının belirlediği bütçe ve bitiş saatiyle çalışan seans.
    
    Telegram komutu: "90 usdt 09:00"
    → budget_usdt=90, end_time=bugün 09:00
    """

    def __init__(self, budget_usdt: float, end_time: datetime):
        self.budget_usdt    = budget_usdt
        self.remaining_usdt = budget_usdt
        self.end_time       = end_time
        self.active         = True
        self.trades: list   = []
        self.start_time     = datetime.now(timezone.utc).replace(tzinfo=None)

    def is_expired(self) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        return now >= self.end_time

    def can_trade(self, amount: float) -> bool:
        return self.active and not self.is_expired() and self.remaining_usdt >= amount

    def record_buy(self, result: dict, amount: float) -> None:
        self.remaining_usdt -= amount
        self.trades.append({**result, "session_cost": amount})

    def summary(self) -> str:
        total_pnl = sum(t.get("pnl_usdt", 0) for t in self.trades)
        lines = [
            f"📋 SEANS ÖZETI",
            f"Bütçe          : {self.budget_usdt:.2f} USDT",
            f"Kullanılan     : {self.budget_usdt - self.remaining_usdt:.2f} USDT",
            f"İşlem sayısı   : {len(self.trades)}",
            f"Tahmini PnL    : {total_pnl:+.2f} USDT",
        ]
        return "\n".join(lines)


_active_session: Optional[TradingSession] = None


def get_active_session() -> Optional[TradingSession]:
    global _active_session
    if _active_session and (_active_session.is_expired() or not _active_session.active):
        _active_session = None
    return _active_session


def start_session(budget_usdt: float, end_time: datetime) -> TradingSession:
    global _active_session
    _active_session = TradingSession(budget_usdt, end_time)
    return _active_session


def end_session() -> Optional[TradingSession]:
    global _active_session
    if _active_session:
        _active_session.active = False
    sess = _active_session
    _active_session = None
    return sess


# ---------------------------------------------------------------------------
# İşlem geçmişi analizi
# ---------------------------------------------------------------------------

def analyze_trade_history(trades: list) -> dict:
    """
    Son N işlemi analiz eder.
    trades: [{'pnl_usdt': float, 'pnl_pct': float, 'symbol': str, ...}, ...]
    """
    if not trades:
        return {}

    wins   = [t for t in trades if t.get("pnl_usdt", 0) > 0]
    losses = [t for t in trades if t.get("pnl_usdt", 0) <= 0]

    total_pnl   = sum(t.get("pnl_usdt", 0) for t in trades)
    avg_win     = sum(t.get("pnl_usdt", 0) for t in wins)   / len(wins)   if wins   else 0
    avg_loss    = sum(t.get("pnl_usdt", 0) for t in losses) / len(losses) if losses else 0
    win_rate    = len(wins) / len(trades) * 100 if trades else 0

    # En çok kazandıran coin
    by_sym: dict = {}
    for t in trades:
        sym = t.get("symbol", "?")
        by_sym.setdefault(sym, 0)
        by_sym[sym] += t.get("pnl_usdt", 0)
    best_coin  = max(by_sym, key=by_sym.get) if by_sym else "-"
    worst_coin = min(by_sym, key=by_sym.get) if by_sym else "-"

    return {
        "count":      len(trades),
        "wins":       len(wins),
        "losses":     len(losses),
        "win_rate":   win_rate,
        "total_pnl":  total_pnl,
        "avg_win":    avg_win,
        "avg_loss":   avg_loss,
        "best_coin":  best_coin,
        "worst_coin": worst_coin,
    }


def format_analysis_report(stats: dict) -> str:
    if not stats:
        return "⚠️ Analiz için yeterli işlem geçmişi yok."

    emoji = "🟢" if stats["total_pnl"] >= 0 else "🔴"
    return "\n".join([
        f"📊 SON {stats['count']} İŞLEM ANALİZİ",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"✅ Kazanan işlem : {stats['wins']}",
        f"❌ Kaybeden işlem: {stats['losses']}",
        f"🎯 Kazanma oranı : %{stats['win_rate']:.1f}",
        f"",
        f"{emoji} Toplam PnL     : {stats['total_pnl']:+.2f} USDT",
        f"📈 Ort. kâr/işlem: +{stats['avg_win']:.2f} USDT",
        f"📉 Ort. zarar/işl: {stats['avg_loss']:.2f} USDT",
        f"",
        f"🏆 En iyi coin   : {stats['best_coin']}",
        f"💀 En kötü coin  : {stats['worst_coin']}",
    ])
