"""
futures_trader.py  —  Binance USDT-M Perpetual Futures
========================================================
Sadece LONG pozisyon açar/kapatır.
Ana bot (main.py) spot sinyallerini futures'a yönlendirir.

Gereksinimler:
  - Binance API key'inde "Enable Futures" izni
  - USDT Futures cüzdanında bakiye
  - .env: FUTURES_ENABLED=true, FUTURES_LEVERAGE=3
"""

import logging
import math
import time
from typing import Optional

from binance.client import Client
from binance.exceptions import BinanceAPIException

logger = logging.getLogger("futures_trader")

# ─── Sabitler ─────────────────────────────────────────────────────────────────
DEFAULT_LEVERAGE   = 3        # 3x kaldıraç
MAX_LEVERAGE       = 5        # Güvenlik üst sınırı
MIN_NOTIONAL_USDT  = 5.0      # Min işlem büyüklüğü
MARGIN_TYPE        = "ISOLATED"   # ISOLATED = risk izole, CROSSED = paylaşımlı


# ─── Yardımcılar ──────────────────────────────────────────────────────────────

def get_futures_balance(client: Client) -> float:
    """USDT-M Futures cüzdan bakiyesi. İzin yoksa sessizce 0 döner."""
    try:
        account = client.futures_account_balance()
        for asset in account:
            if asset["asset"] == "USDT":
                return float(asset["availableBalance"])
    except BinanceAPIException as e:
        if e.code in (-2015, -2014, -1003):
            logger.warning("Futures bakiye sorgulanamadı (izin/key sorunu): %s", e.code)
        else:
            logger.error("futures_balance hata: %s", e)
    except Exception as e:
        logger.warning("futures_balance hata: %s", e)
    return 0.0


def get_futures_account_info(client: Client) -> dict:
    """Futures hesap özeti."""
    try:
        info = client.futures_account()
        return {
            "total_wallet":     float(info.get("totalWalletBalance", 0)),
            "available":        float(info.get("availableBalance", 0)),
            "unrealized_pnl":   float(info.get("totalUnrealizedProfit", 0)),
            "margin_ratio":     float(info.get("totalMaintMargin", 0)),
        }
    except Exception as e:
        logger.error("futures_account_info hata: %s", e)
        return {}


def get_open_futures_positions(client: Client) -> list:
    """Açık futures pozisyonlarını listeler. İzin yoksa boş liste döner."""
    try:
        positions = client.futures_position_information()
        open_pos = []
        for p in positions:
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                open_pos.append({
                    "symbol":        p["symbol"],
                    "side":          "LONG" if amt > 0 else "SHORT",
                    "qty":           abs(amt),
                    "entry_price":   float(p["entryPrice"]),
                    "mark_price":    float(p["markPrice"]),
                    "unrealized_pnl": float(p["unRealizedProfit"]),
                    "leverage":      int(p["leverage"]),
                    "liquidation":   float(p.get("liquidationPrice", 0)),
                    "notional":      abs(float(p.get("notional", 0))),
                })
        return open_pos
    except BinanceAPIException as e:
        if e.code in (-2015, -2014):
            logger.warning("Futures pozisyon sorgulanamadı (izin sorunu): %s", e.code)
        else:
            logger.error("get_open_futures_positions hata: %s", e)
        return []
    except Exception as e:
        logger.warning("get_open_futures_positions hata: %s", e)
        return []


def get_futures_position(client: Client, symbol: str) -> Optional[dict]:
    """Belirli bir sembol için açık pozisyon."""
    positions = get_open_futures_positions(client)
    for p in positions:
        if p["symbol"] == symbol:
            return p
    return None


def _set_leverage(client: Client, symbol: str, leverage: int) -> bool:
    """Sembol için kaldıraç ayarlar."""
    try:
        lev = max(1, min(leverage, MAX_LEVERAGE))
        client.futures_change_leverage(symbol=symbol, leverage=lev)
        return True
    except BinanceAPIException as e:
        if "No need to change leverage" in str(e):
            return True
        logger.warning("_set_leverage %s: %s", symbol, e)
        return False


def _set_margin_type(client: Client, symbol: str) -> bool:
    """ISOLATED margin tipini ayarlar."""
    try:
        client.futures_change_margin_type(symbol=symbol, marginType=MARGIN_TYPE)
        return True
    except BinanceAPIException as e:
        if "No need to change margin type" in str(e):
            return True
        logger.warning("_set_margin_type %s: %s", symbol, e)
        return False


def _get_futures_symbol_info(client: Client, symbol: str) -> Optional[dict]:
    """Futures sembol kurallarını döner (step size, min qty)."""
    try:
        info = client.futures_exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                result = {"filters": {}}
                for f in s.get("filters", []):
                    ft = f["filterType"]
                    if ft == "LOT_SIZE":
                        result["step_size"]  = float(f["stepSize"])
                        result["min_qty"]    = float(f["minQty"])
                    elif ft == "MIN_NOTIONAL":
                        result["min_notional"] = float(f.get("notional", MIN_NOTIONAL_USDT))
                    elif ft == "PRICE_FILTER":
                        result["tick_size"]  = float(f["tickSize"])
                return result
    except Exception as e:
        logger.error("_get_futures_symbol_info %s: %s", symbol, e)
    return None


def _round_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    precision = max(0, round(-math.log10(step)))
    return round(math.floor(qty / step) * step, precision)


# ─── Ana işlem fonksiyonları ───────────────────────────────────────────────────

def futures_long(
    client: Client,
    symbol: str,
    usdt_amount: float,
    leverage: int = DEFAULT_LEVERAGE,
    stop_pct: float = 0.05,
) -> Optional[dict]:
    """
    USDT-M Perpetual LONG pozisyon açar.
    usdt_amount: Margin olarak kullanılacak USDT (kaldıraç ile çarpılır)
    stop_pct: Stop-loss yüzdesi (0.05 = %5)
    """
    try:
        leverage = max(1, min(leverage, MAX_LEVERAGE))
        _set_margin_type(client, symbol)
        _set_leverage(client, symbol, leverage)

        # Güncel fiyat
        ticker    = client.futures_symbol_ticker(symbol=symbol)
        price     = float(ticker["price"])
        if price <= 0:
            logger.error("futures_long: %s fiyat alınamadı", symbol)
            return None

        # Notional (pozisyon büyüklüğü) = margin × leverage
        notional  = usdt_amount * leverage
        sym_info  = _get_futures_symbol_info(client, symbol)
        step      = sym_info.get("step_size", 0.001) if sym_info else 0.001
        min_qty   = sym_info.get("min_qty", 0.001) if sym_info else 0.001
        min_not   = sym_info.get("min_notional", MIN_NOTIONAL_USDT) if sym_info else MIN_NOTIONAL_USDT

        qty = _round_step(notional / price, step)
        if qty < min_qty:
            logger.warning("futures_long %s: qty %.6f < min %.6f", symbol, qty, min_qty)
            return None
        if qty * price < min_not:
            logger.warning("futures_long %s: notional %.2f < min %.2f", symbol, qty * price, min_not)
            return None

        # Piyasa emri
        order = client.futures_create_order(
            symbol    = symbol,
            side      = "BUY",
            type      = "MARKET",
            quantity  = qty,
        )

        fill_price = float(order.get("avgPrice") or price)
        if fill_price <= 0:
            fill_price = price

        # Stop-loss emri (STOP_MARKET)
        stop_price = round(fill_price * (1 - stop_pct), 8)
        tick       = sym_info.get("tick_size", 0.0001) if sym_info else 0.0001
        tick_prec  = max(0, round(-math.log10(tick)))
        stop_price = round(stop_price, tick_prec)
        try:
            client.futures_create_order(
                symbol        = symbol,
                side          = "SELL",
                type          = "STOP_MARKET",
                stopPrice     = stop_price,
                closePosition = True,
            )
            logger.info("Stop-loss emri: %s @ %.6f", symbol, stop_price)
        except Exception as e:
            logger.warning("Stop-loss emri başarısız %s: %s", symbol, e)

        result = {
            "symbol":      symbol,
            "side":        "LONG",
            "qty":         qty,
            "avg_price":   fill_price,
            "notional":    round(qty * fill_price, 4),
            "margin":      round(usdt_amount, 4),
            "leverage":    leverage,
            "stop_price":  stop_price,
            "stop_pct":    stop_pct,
            "order_id":    order.get("orderId"),
        }
        logger.info(
            "FUTURES LONG açıldı: %s  %.4f adet @ %.6f  kaldıraç:%dx  stop:%.6f",
            symbol, qty, fill_price, leverage, stop_price,
        )
        return result

    except BinanceAPIException as e:
        logger.error("futures_long BinanceAPIException %s: %s", symbol, e)
        return None
    except Exception as e:
        logger.error("futures_long hata %s: %s", symbol, e)
        return None


def futures_close_long(client: Client, symbol: str) -> Optional[dict]:
    """Açık LONG pozisyonun tamamını kapatır."""
    try:
        pos = get_futures_position(client, symbol)
        if not pos or pos["side"] != "LONG":
            logger.warning("futures_close_long: %s açık LONG pozisyon yok", symbol)
            return None

        qty = pos["qty"]
        # Önce açık stop emirlerini iptal et
        try:
            client.futures_cancel_all_open_orders(symbol=symbol)
        except Exception:
            pass

        order = client.futures_create_order(
            symbol    = symbol,
            side      = "SELL",
            type      = "MARKET",
            quantity  = qty,
            reduceOnly= True,
        )

        fill = float(order.get("avgPrice") or pos["mark_price"])
        pnl  = (fill - pos["entry_price"]) * qty
        result = {
            "symbol":      symbol,
            "qty":         qty,
            "exit_price":  fill,
            "entry_price": pos["entry_price"],
            "pnl_usdt":    round(pnl, 4),
            "pnl_pct":     round((fill - pos["entry_price"]) / pos["entry_price"] * 100, 2),
            "order_id":    order.get("orderId"),
        }
        logger.info(
            "FUTURES LONG kapatıldı: %s  PnL: %+.4f USDT (%+.2f%%)",
            symbol, pnl, result["pnl_pct"],
        )
        return result

    except BinanceAPIException as e:
        logger.error("futures_close_long BinanceAPIException %s: %s", symbol, e)
        return None
    except Exception as e:
        logger.error("futures_close_long hata %s: %s", symbol, e)
        return None


def futures_close_all(client: Client) -> list:
    """Tüm açık futures pozisyonlarını kapatır."""
    positions = get_open_futures_positions(client)
    results = []
    for pos in positions:
        if pos["side"] == "LONG":
            r = futures_close_long(client, pos["symbol"])
            if r:
                results.append(r)
        time.sleep(0.3)
    return results


def format_futures_position(pos: dict) -> str:
    """Pozisyonu okunabilir formatta döner."""
    pnl_sign = "+" if pos["unrealized_pnl"] >= 0 else ""
    pnl_pct  = (pos["mark_price"] - pos["entry_price"]) / pos["entry_price"] * 100
    return (
        f"📊 {pos['symbol'].replace('USDT','')} — LONG {pos['leverage']}x\n"
        f"  Giriş   : {pos['entry_price']:.6f}\n"
        f"  Mark    : {pos['mark_price']:.6f}\n"
        f"  PnL     : {pnl_sign}{pos['unrealized_pnl']:.4f} USDT  ({pnl_sign}{pnl_pct:.2f}%)\n"
        f"  Likidasy: {pos['liquidation']:.6f}\n"
        f"  Miktar  : {pos['qty']:.6f}"
    )
