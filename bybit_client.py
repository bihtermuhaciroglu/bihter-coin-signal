"""
bybit_client.py  —  Bybit V5 API wrapper
=========================================
Binance client metodlarını taklit eder, mevcut kod minimum değişiklikle
Bybit ile çalışır.

Kline interval mapping:
  Binance "1m"→"1", "5m"→"5", "15m"→"15", "30m"→"30",
          "1h"→"60", "2h"→"120", "4h"→"240", "1d"→"D"
"""

import logging
import math
import time
from typing import Optional

logger = logging.getLogger("bybit_client")

# Kline interval dönüşümü
_INTERVAL_MAP = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
    # Binance Client sabitleri
    "KLINE_INTERVAL_1MINUTE":  "1",
    "KLINE_INTERVAL_5MINUTE":  "5",
    "KLINE_INTERVAL_15MINUTE": "15",
    "KLINE_INTERVAL_1HOUR":    "60",
    "KLINE_INTERVAL_4HOUR":    "240",
    "KLINE_INTERVAL_1DAY":     "D",
}

# Binance Client sabitlerini kopyala
KLINE_INTERVAL_1MINUTE  = "1m"
KLINE_INTERVAL_5MINUTE  = "5m"
KLINE_INTERVAL_15MINUTE = "15m"
KLINE_INTERVAL_1HOUR    = "1h"
KLINE_INTERVAL_4HOUR    = "4h"
KLINE_INTERVAL_1DAY     = "1d"


def _to_bybit_interval(interval: str) -> str:
    return _INTERVAL_MAP.get(interval, interval)


class BybitClient:
    """
    Bybit V5 HTTP wrapper — Binance client metodlarını taklit eder.
    """

    def __init__(self, api_key: str = "", api_secret: str = "", testnet: bool = False):
        try:
            from pybit.unified_trading import HTTP
            self._session = HTTP(
                testnet   = testnet,
                api_key   = api_key,
                api_secret= api_secret,
            )
        except ImportError:
            raise ImportError("pybit kurulu değil. Çalıştır: pip install pybit")
        self._api_key    = api_key
        self._api_secret = api_secret
        logger.info("BybitClient oluşturuldu (testnet=%s)", testnet)

    # ── Bağlantı testi ────────────────────────────────────────────────────────
    def ping(self) -> dict:
        try:
            r = self._session.get_server_time()
            return {}
        except Exception as e:
            raise ConnectionError(f"Bybit ping başarısız: {e}")

    # ── Ticker ────────────────────────────────────────────────────────────────
    def get_ticker(self) -> list:
        """
        Binance get_ticker() gibi tüm USDT spot sembollerinin 24h istatistiklerini döner.
        Her eleman: symbol, lastPrice, priceChangePercent, quoteVolume
        """
        try:
            r = self._session.get_tickers(category="spot")
            result = []
            for t in r["result"]["list"]:
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"):
                    continue
                # Bybit price24hPcnt ondalık (0.012 = %1.2), Binance yüzde verir
                pct_raw = float(t.get("price24hPcnt", "0") or "0")
                pct = pct_raw * 100 if abs(pct_raw) <= 1.0 else pct_raw
                result.append({
                    "symbol":             sym,
                    "lastPrice":          t.get("lastPrice", "0"),
                    "priceChangePercent": str(round(pct, 4)),
                    "quoteVolume":        t.get("turnover24h", "0"),
                    "volume":             t.get("volume24h", "0"),
                    "highPrice":          t.get("highPrice24h", "0"),
                    "lowPrice":           t.get("lowPrice24h", "0"),
                })
            return result
        except Exception as e:
            logger.error("get_ticker hata: %s", e)
            return []

    # ── Klines ────────────────────────────────────────────────────────────────
    def get_klines(self, symbol: str, interval: str,
                   limit: int = 200, startTime: int = None, endTime: int = None) -> list:
        """
        Binance get_klines() formatında OHLCV döner.
        Her eleman: [open_time, open, high, low, close, volume, ...]
        """
        try:
            bybit_interval = _to_bybit_interval(interval)
            params = {
                "category": "spot",
                "symbol":   symbol,
                "interval": bybit_interval,
                "limit":    min(limit, 1000),
            }
            if startTime:
                params["start"] = startTime
            if endTime:
                params["end"] = endTime

            r = self._session.get_kline(**params)
            raw = r["result"]["list"]
            # Bybit: [startTime, open, high, low, close, volume, turnover]
            # Binance: [openTime, open, high, low, close, volume, closeTime, ...]
            result = []
            for k in reversed(raw):   # Bybit newest first, Binance oldest first
                result.append([
                    int(k[0]),   # open_time ms
                    k[1],        # open
                    k[2],        # high
                    k[3],        # low
                    k[4],        # close
                    k[5],        # volume
                    int(k[0]) + 59999,  # close_time (approx)
                    k[6],        # quote volume (turnover)
                    0, "0", "0", "0",
                ])
            return result
        except Exception as e:
            logger.error("get_klines %s hata: %s", symbol, e)
            return []

    # ── Hesap / Bakiye ────────────────────────────────────────────────────────
    def get_account(self) -> dict:
        """Spot bakiyelerini Binance formatında döner."""
        try:
            r = self._session.get_wallet_balance(accountType="UNIFIED")
            balances = []
            for coin in r["result"]["list"][0].get("coin", []):
                free = (coin.get("availableToWithdraw")
                        or coin.get("availableToBorrow")
                        or coin.get("walletBalance", "0"))
                balances.append({
                    "asset":  coin["coin"],
                    "free":   free,
                    "locked": "0",
                })
            return {"balances": balances}
        except Exception as e:
            logger.error("get_account hata: %s", e)
            return {"balances": []}

    def futures_account_balance(self) -> list:
        """USDT-M Futures bakiyesi — Bybit Unified hesabından."""
        try:
            r = self._session.get_wallet_balance(accountType="UNIFIED")
            result = []
            for coin in r["result"]["list"][0].get("coin", []):
                if coin["coin"] == "USDT":
                    avail = (coin.get("availableToWithdraw")
                             or coin.get("availableToBorrow")
                             or coin.get("walletBalance", "0"))
                    result.append({
                        "asset":            "USDT",
                        "availableBalance": avail,
                        "balance":          coin.get("walletBalance", "0"),
                    })
            return result
        except Exception as e:
            logger.warning("futures_account_balance hata: %s", e)
            return []

    # ── Emir ──────────────────────────────────────────────────────────────────
    def order_market_buy(self, symbol: str, quoteOrderQty: float) -> dict:
        """USDT ile market alım."""
        try:
            r = self._session.place_order(
                category    = "spot",
                symbol      = symbol,
                side        = "Buy",
                orderType   = "Market",
                qty         = str(quoteOrderQty),
                marketUnit  = "quoteCoin",   # USDT miktarı
            )
            return r["result"]
        except Exception as e:
            logger.error("order_market_buy %s hata: %s", symbol, e)
            raise

    def order_market_sell(self, symbol: str, quantity: float) -> dict:
        """Coin miktarı ile market satış."""
        try:
            r = self._session.place_order(
                category  = "spot",
                symbol    = symbol,
                side      = "Sell",
                orderType = "Market",
                qty       = str(quantity),
                marketUnit= "baseCoin",
            )
            return r["result"]
        except Exception as e:
            logger.error("order_market_sell %s hata: %s", symbol, e)
            raise

    # ── Futures emir ──────────────────────────────────────────────────────────
    def futures_create_order(self, symbol: str, side: str, type: str,
                              quantity: float = None, stopPrice: float = None,
                              closePosition: bool = False, reduceOnly: bool = False,
                              **kwargs) -> dict:
        try:
            params = {
                "category": "linear",
                "symbol":   symbol,
                "side":     side.capitalize(),
                "orderType": "Market" if type in ("MARKET", "STOP_MARKET") else type,
            }
            if closePosition:
                params["reduceOnly"] = True
                params["qty"] = "0"
                params["closeOnTrigger"] = True
            elif quantity:
                params["qty"] = str(quantity)
            if stopPrice and type == "STOP_MARKET":
                params["triggerPrice"] = str(stopPrice)
                params["triggerBy"]    = "MarkPrice"
                params["orderType"]    = "Market"
                params["reduceOnly"]   = True
            if reduceOnly:
                params["reduceOnly"] = True

            r = self._session.place_order(**params)
            return r["result"]
        except Exception as e:
            logger.error("futures_create_order %s hata: %s", symbol, e)
            raise

    def futures_change_leverage(self, symbol: str, leverage: int) -> dict:
        try:
            r = self._session.set_leverage(
                category     = "linear",
                symbol       = symbol,
                buyLeverage  = str(leverage),
                sellLeverage = str(leverage),
            )
            return r
        except Exception as e:
            logger.warning("futures_change_leverage %s: %s", symbol, e)
            return {}

    def futures_change_margin_type(self, symbol: str, marginType: str) -> dict:
        try:
            mode = 0 if marginType == "CROSSED" else 1  # 1=isolated
            r = self._session.switch_margin_mode(
                category   = "linear",
                symbol     = symbol,
                tradeMode  = mode,
                buyLeverage= "3",
                sellLeverage="3",
            )
            return r
        except Exception as e:
            logger.warning("futures_change_margin_type %s: %s", symbol, e)
            return {}

    def futures_position_information(self, symbol: str = None) -> list:
        try:
            params = {"category": "linear"}
            if symbol:
                params["symbol"] = symbol
            r = self._session.get_positions(**params)
            result = []
            for p in r["result"]["list"]:
                size = float(p.get("size", 0))
                side = p.get("side", "None")
                amt  = size if side == "Buy" else -size if side == "Sell" else 0
                result.append({
                    "symbol":          p["symbol"],
                    "positionAmt":     str(amt),
                    "entryPrice":      p.get("avgPrice", "0"),
                    "markPrice":       p.get("markPrice", "0"),
                    "unRealizedProfit":p.get("unrealisedPnl", "0"),
                    "leverage":        p.get("leverage", "1"),
                    "liquidationPrice":p.get("liqPrice", "0"),
                    "notional":        p.get("positionValue", "0"),
                })
            return result
        except Exception as e:
            logger.error("futures_position_information hata: %s", e)
            return []

    def futures_cancel_all_open_orders(self, symbol: str) -> dict:
        try:
            r = self._session.cancel_all_orders(category="linear", symbol=symbol)
            return r
        except Exception as e:
            logger.warning("futures_cancel_all_open_orders %s: %s", symbol, e)
            return {}

    def futures_symbol_ticker(self, symbol: str) -> dict:
        try:
            r = self._session.get_tickers(category="linear", symbol=symbol)
            t = r["result"]["list"][0]
            return {"price": t.get("lastPrice", "0"), "symbol": symbol}
        except Exception as e:
            logger.error("futures_symbol_ticker %s: %s", symbol, e)
            return {"price": "0"}

    def futures_mark_price(self) -> list:
        """Funding rate verisi için tüm linear semboller."""
        try:
            r = self._session.get_tickers(category="linear")
            result = []
            for t in r["result"]["list"]:
                result.append({
                    "symbol":          t.get("symbol", ""),
                    "lastFundingRate": t.get("fundingRate", "0"),
                })
            return result
        except Exception as e:
            logger.warning("futures_mark_price hata: %s", e)
            return []

    def futures_exchange_info(self) -> dict:
        """Symbol info — lot size, tick size."""
        try:
            r = self._session.get_instruments_info(category="linear")
            symbols = []
            for s in r["result"]["list"]:
                filters = []
                lot = s.get("lotSizeFilter", {})
                price = s.get("priceFilter", {})
                if lot:
                    filters.append({
                        "filterType": "LOT_SIZE",
                        "stepSize":   lot.get("qtyStep", "0.001"),
                        "minQty":     lot.get("minOrderQty", "0.001"),
                    })
                if price:
                    filters.append({
                        "filterType": "PRICE_FILTER",
                        "tickSize":   price.get("tickSize", "0.0001"),
                    })
                symbols.append({"symbol": s["symbol"], "filters": filters})
            return {"symbols": symbols}
        except Exception as e:
            logger.error("futures_exchange_info hata: %s", e)
            return {"symbols": []}

    # ── Exchange info (spot) ──────────────────────────────────────────────────
    def get_exchange_info(self) -> dict:
        """Spot sembol bilgileri."""
        try:
            r = self._session.get_instruments_info(category="spot")
            symbols = []
            for s in r["result"]["list"]:
                filters = []
                lot = s.get("lotSizeFilter", {})
                price = s.get("priceFilter", {})
                min_not = s.get("minNotionalFilter", {})
                if lot:
                    filters.append({
                        "filterType": "LOT_SIZE",
                        "stepSize":   lot.get("basePrecision", "0.00001"),
                        "minQty":     lot.get("minOrderQty", "0.00001"),
                    })
                if price:
                    filters.append({
                        "filterType": "PRICE_FILTER",
                        "tickSize":   price.get("tickSize", "0.0001"),
                    })
                if min_not:
                    filters.append({
                        "filterType": "MIN_NOTIONAL",
                        "minNotional": min_not.get("minNotionalValue", "1"),
                    })
                symbols.append({"symbol": s["symbol"], "filters": filters})
            return {"symbols": symbols}
        except Exception as e:
            logger.error("get_exchange_info hata: %s", e)
            return {"symbols": []}

    def get_order_book(self, symbol: str, limit: int = 5) -> dict:
        try:
            r = self._session.get_orderbook(category="spot", symbol=symbol, limit=limit)
            return {
                "bids": r["result"]["b"],
                "asks": r["result"]["a"],
            }
        except Exception:
            return {"bids": [], "asks": []}
