import os
import time
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

SCAN_INTERVAL = 300
REPORT_INTERVAL = 3600
SIGNAL_COOLDOWN_HOURS = 12
MIN_VOLUME_USDT = 10_000_000
STATE_FILE = "state.json"

MAX_NEW_SIGNALS = 3
MIN_BUY_SCORE = 85


def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": text})


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except:
        return {
            "signals": {},
            "last_report": 0,
            "closed": []
        }


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def get_balances():
    account = client.get_account()
    balances = {}

    for b in account["balances"]:
        free = float(b["free"])
        locked = float(b["locked"])
        total = free + locked

        if total > 0:
            balances[b["asset"]] = total

    return balances


def get_usdt_balance(balances):
    return balances.get("USDT", 0)


def get_tickers_map():
    tickers = client.get_ticker()
    return {t["symbol"]: t for t in tickers}


def safe_float(value):
    try:
        return float(value)
    except:
        return 0.0


def score_coin(ticker, btc_change):
    symbol = ticker["symbol"]

    if not symbol.endswith("USDT"):
        return None
    
    if symbol in ["BTCUSDT", "ETHUSDT", "BNBUSDT"]:
    return None

    banned = ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT", "FDUSD", "TUSD", "USDC"]
    if any(x in symbol for x in banned):
        return None

    price = safe_float(ticker["lastPrice"])
    change = safe_float(ticker["priceChangePercent"])
    volume = safe_float(ticker["quoteVolume"])

    if volume < MIN_VOLUME_USDT:
        return None

    score = 50

    if btc_change < -2:
        score -= 20
    elif btc_change > 1:
        score += 8

    if 1.5 <= change <= 6:
        score += 22
    elif 6 < change <= 12:
        score += 12
    elif 12 < change <= 18:
        score -= 5
    elif change > 18:
        score -= 25
    elif -2 <= change < 1.5:
        score += 5
    elif change < -4:
        score -= 15

    if volume > 50_000_000:
        score += 10
    if volume > 100_000_000:
        score += 8
    if volume > 300_000_000:
        score += 5

    score = max(0, min(score, 100))

    return {
        "symbol": symbol,
        "price": price,
        "change": change,
        "volume": volume,
        "score": score
    }


def position_size(usdt_balance, score):
    if usdt_balance <= 0:
        return 0

    if score >= 95:
        percent = 0.20
    elif score >= 90:
        percent = 0.15
    elif score >= 85:
        percent = 0.10
    else:
        percent = 0

    return round(usdt_balance * percent, 2)


def should_send_new_signal(symbol, score, state, balances):
    asset = symbol.replace("USDT", "")

    if asset in balances and balances[asset] > 0:
        return False

    existing = state["signals"].get(symbol)
    if existing:
        last_time = datetime.fromisoformat(existing["time"])
        last_score = existing["score"]

        if datetime.utcnow() - last_time < timedelta(hours=SIGNAL_COOLDOWN_HOURS):
            if score < last_score + 8:
                return False

    return True


def create_signal(coin, usdt_balance):
    entry = coin["price"]
    stop = entry * 0.97
    target1 = entry * 1.04
    target2 = entry * 1.08
    amount = position_size(usdt_balance, coin["score"])

    return {
        "symbol": coin["symbol"],
        "time": datetime.utcnow().isoformat(),
        "entry": entry,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "score": coin["score"],
        "amount": amount,
        "status": "active"
    }


def send_buy_signals(new_signals, usdt_balance):
    msg = f"🚀 AKILLI AL SİNYALİ\n\n💰 Spot USDT: {usdt_balance:.2f}\n\n"

    if usdt_balance == 0:
        msg += "⚠️ USDT Spot cüzdanda 0 görünüyor. Para Funding/Earn tarafındaysa Spot'a transfer etmelisin.\n\n"

    for i, s in enumerate(new_signals, 1):
        msg += f"""{i}) {s['symbol']}
Skor: {s['score']}/100
Giriş: {s['entry']:.6f}
Önerilen Tutar: {s['amount']} USDT

Stop: {s['stop']:.6f}
Hedef 1: {s['target1']:.6f}
Hedef 2: {s['target2']:.6f}

"""

    msg += "⚠️ Otomatik alım değildir. İşlem öncesi grafiği kontrol et."
    send_telegram(msg)


def check_active_signals(state, tickers_map):
    updates = []

    for symbol, signal in list(state["signals"].items()):
        if signal.get("status") != "active":
            continue

        ticker = tickers_map.get(symbol)
        if not ticker:
            continue

        price = safe_float(ticker["lastPrice"])
        entry = signal["entry"]
        pnl = ((price - entry) / entry) * 100

        if price >= signal["target2"]:
            signal["status"] = "target2"
            updates.append(f"🎯 HEDEF 2 GELDİ\n{symbol}\nKâr: %{pnl:.2f}")
        elif price >= signal["target1"] and not signal.get("target1_notified"):
            signal["target1_notified"] = True
            updates.append(f"✅ HEDEF 1 GELDİ\n{symbol}\nKâr: %{pnl:.2f}\nKâr almayı değerlendirebilirsin.")
        elif price <= signal["stop"]:
            signal["status"] = "stopped"
            updates.append(f"🛑 STOP SEVİYESİ\n{symbol}\nZarar: %{pnl:.2f}")

    for u in updates:
        send_telegram(u)


def portfolio_report(balances, scored):
    usdt = get_usdt_balance(balances)
    top = scored[:3]

    msg = f"📊 PORTFÖY RAPORU\n\n💰 Spot USDT: {usdt:.2f}\n\n"

    msg += "🔥 En güçlü fırsatlar:\n"
    for i, c in enumerate(top, 1):
        msg += f"{i}) {c['symbol']} | Skor: {c['score']}/100 | 24s: %{c['change']:.2f}\n"

    holdings = []

    for asset, amount in balances.items():
        if asset == "USDT":
            continue

        symbol = asset + "USDT"
        match = next((x for x in scored if x["symbol"] == symbol), None)

        if match:
            holdings.append({
                "symbol": symbol,
                "amount": amount,
                "score": match["score"],
                "change": match["change"]
            })

    if holdings:
        holdings.sort(key=lambda x: x["score"], reverse=True)
        msg += "\n📌 Elindeki coinler:\n"

        for h in holdings:
            if h["score"] >= 80:
                action = "TUT"
            elif h["score"] >= 65:
                action = "İZLE"
            else:
                action = "ZAYIF"

            msg += f"- {h['symbol']} | {h['score']}/100 | {action} | 24s: %{h['change']:.2f}\n"

        weakest = holdings[-1]
        strongest = top[0]

        if strongest["score"] - weakest["score"] >= 20:
            msg += f"""

🔄 ROTASYON ÖNERİSİ

Zayıf: {weakest['symbol']} ({weakest['score']}/100)
Güçlü: {strongest['symbol']} ({strongest['score']}/100)

Öneri:
{weakest['symbol']} pozisyonunun bir kısmını {strongest['symbol']} tarafına taşımayı değerlendirebilirsin.
"""
    else:
        msg += "\nAçık coin pozisyonu görünmüyor."

    msg += "\n\n⚠️ Bu yatırım tavsiyesi değildir."
    send_telegram(msg)


def run_bot():
    send_telegram("✅ Bihter Coin Signal Smart v3 başladı.")

    while True:
        try:
            state = load_state()

            balances = get_balances()
            usdt_balance = get_usdt_balance(balances)

            tickers_map = get_tickers_map()
            tickers = list(tickers_map.values())

            btc = tickers_map.get("BTCUSDT")
            btc_change = safe_float(btc["priceChangePercent"]) if btc else 0

            scored = []
            for ticker in tickers:
                coin = score_coin(ticker, btc_change)
                if coin:
                    scored.append(coin)

            scored.sort(key=lambda x: x["score"], reverse=True)

            check_active_signals(state, tickers_map)

            new_signals = []

            for coin in scored:
                if coin["score"] >= MIN_BUY_SCORE and should_send_new_signal(coin["symbol"], coin["score"], state, balances):
                    signal = create_signal(coin, usdt_balance)
                    state["signals"][coin["symbol"]] = signal
                    new_signals.append(signal)

                if len(new_signals) >= MAX_NEW_SIGNALS:
                    break

            if new_signals:
                send_buy_signals(new_signals, usdt_balance)

            now = time.time()
            if now - state.get("last_report", 0) >= REPORT_INTERVAL:
                portfolio_report(balances, scored)
                state["last_report"] = now

            save_state(state)

        except Exception as e:
            send_telegram(f"⚠️ Bot hatası:\n{str(e)}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    run_bot()
