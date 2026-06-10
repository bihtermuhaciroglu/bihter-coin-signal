import os
import requests
from dotenv import load_dotenv
from binance.client import Client

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)

account = client.get_account()

usdt_balance = 0.0

for balance in account["balances"]:
    if balance["asset"] == "USDT":
        usdt_balance = float(balance["free"])
        break

message = f"""💰 Hesap Özeti

USDT Bakiyesi: {usdt_balance:.2f}

Bihter Coin Signal Binance bağlantısı başarılı ✅
"""

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

requests.post(
    url,
    json={
        "chat_id": CHAT_ID,
        "text": message
    }
)

print("Mesaj gönderildi.")
