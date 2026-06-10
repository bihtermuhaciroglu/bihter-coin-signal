import os
import time
import requests
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

MIN_VOLUME_USDT = 10_000_000
MAX_SIGNALS = 3

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": CHAT_ID, "text": message})

def get_usdt_balance():
    account = client.get_account()
    for b in account["balances"]:
        if b["asset"] == "USDT":
            return float(b["free"])
    return 0.0

def score_coin(ticker):
    symbol = ticker["symbol"]
    change = float(ticker["priceChangePercent"])
    volume = float(ticker["quoteVolume"])

    if not symbol.endswith("USDT"):
        return None
    if any(x in symbol for x in ["UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT"]):
        return None
    if volume < MIN_VOLUME_USDT:
        return None

    score = 50

    if change > 2:
        score += 10
    if change > 5:
        score += 15
    if change > 10:
        score -= 10
    if -2 < change < 8:
        score += 10
    if volume > 50_000_000:
        score += 10
    if volume > 100_000_000:
        score += 10

    return {
        "symbol": symbol,
        "price": float(ticker["lastPrice"]),
        "change": change,
        "volume": volume,
        "score": min(score, 100)
    }

def position_size(balance, score):
    if score >= 95:
        percent = 0.20
    elif score >= 90:
        percent = 0.15
    elif score >= 85:
        percent = 0.10
    else:
        percent = 0

    amount = balance * percent
    return round(amount, 2)

def main():
    usdt_balance = get_usdt_balance()
    tickers = client.get_ticker()

    scored = []
    for ticker in tickers:
        result = score_coin(ticker)
        if result:
            scored.append(result)

    scored.sort(key=lambda x: x["score"], reverse=True)
    best = [x for x in scored if x["score"] >= 85][:MAX_SIGNALS]

    if not best:
        print("Güçlü sinyal yok.")
        return

    message = f"🚀 Bihter Coin Signal\n\n💰 USDT Bakiye: {usdt_balance:.2f}\n\n"

    for i, coin in enumerate(best, 1):
        amount = position_size(usdt_balance, coin["score"])
        target1 = coin["price"] * 1.04
        target2 = coin["price"] * 1.08
        stop = coin["price"] * 0.97

        message += f"""{i}) {coin['symbol']}
Skor: {coin['score']}/100
Fiyat: {coin['price']:.6f}
24s Değişim: %{coin['change']:.2f}
Önerilen Tutar: {amount} USDT

Stop: {stop:.6f}
Hedef 1: {target1:.6f}
Hedef 2: {target2:.6f}

"""

    message += "⚠️ Bu otomatik alım değildir. İşlem yapmadan önce grafiği kontrol et."

    send_telegram(message)
    print("Sinyal mesajı gönderildi.")

if __name__ == "__main__":
    main()
