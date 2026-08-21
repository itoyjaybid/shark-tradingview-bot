Python
from fastapi import FastAPI, Request, HTTPException
import os
import requests

app = FastAPI()

SHARK_API_KEY = os.getenv("SHARK_API_KEY", "")
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "")
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY")

@app.get("/")
def home():
    return {"status": "awake", "service": "Shark Trading Bot"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()

    # Verify passphrase
    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Invalid secret passphrase")

    action = data.get("action")
    symbol = data.get("symbol")
    quantity = data.get("quantity")
    price = data.get("price")
    sl_price = data.get("sl_price")

    print(f"Signal Received -> Action: {action} | Symbol: {symbol} | Price: {price} | SL: {sl_price}")

    # Shark Exchange API logic goes here
    return {"status": "success", "data": data}
