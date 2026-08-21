from fastapi import FastAPI, Request, HTTPException
import os
import time
import json
import hmac
import hashlib
import requests

app = FastAPI()

SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "")
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "")
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY")


def generate_signature(secret: str, data: str) -> str:
    return hmac.new(secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def execute_shark_order(side: str, symbol: str, quantity: float, stop_loss_price: float = None, reduce_only: bool = False):
    endpoint = f"{SHARK_BASE_URL}/v1/order/place-order"
    timestamp = str(int(time.time() * 1000))
    
    # Strip TradingView perpetual suffix (e.g., BTCUSDT.P -> BTCUSDT)
    clean_symbol = symbol.replace(".P", "").replace(".p", "")
    
    payload = {
        "timestamp": timestamp,
        "placeType": "ORDER_FORM",
        "quantity": float(quantity),
        "side": side.upper(),
        "symbol": clean_symbol,
        "type": "MARKET",
        "reduceOnly": reduce_only,
        "marginAsset": "INR",
        "deviceType": "WEB",
        "userCategory": "EXTERNAL"
    }

    if stop_loss_price:
        payload["stopLossPrice"] = float(stop_loss_price)

    data_to_sign = json.dumps(payload, separators=(",", ":"))
    signature = generate_signature(SHARK_API_SECRET, data_to_sign)

    headers = {
        "api-key": SHARK_API_KEY,
        "signature": signature,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(endpoint, json=payload, headers=headers)
        print(f"Shark API Response Status: {response.status_code}")
        print(f"Shark API Response Body: {response.text}")
        return response.json()
    except Exception as e:
        print(f"Order Execution Error: {str(e)}")
        return {"error": str(e)}


@app.get("/")
def home():
    return {"status": "awake", "service": "Shark Trading Bot"}


@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()

    # 1. Verify passphrase
    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Invalid secret passphrase")

    action = data.get("action", "").upper()
    symbol = data.get("symbol", "BTCUSDT")
    quantity = float(data.get("quantity", 0.002))
    sl_price = data.get("sl_price")

    print(f"Signal Received -> Action: {action} | Symbol: {symbol} | Price: {data.get('price')} | SL: {sl_price}")

    # 2. Route all strategy actions
    if action == "BUY":
        result = execute_shark_order(side="BUY", symbol=symbol, quantity=quantity, stop_loss_price=sl_price, reduce_only=False)

    elif action == "SELL":
        result = execute_shark_order(side="SELL", symbol=symbol, quantity=quantity, stop_loss_price=sl_price, reduce_only=False)

    elif action in ["SL_EXIT", "EXIT", "CLOSE"]:
        # Market close existing position
        result = execute_shark_order(side="SELL", symbol=symbol, quantity=quantity, reduce_only=True)

    elif action == "UPDATE_SL":
        # Trailing Stop-Loss update
        print(f"Updating Trailing Stop Loss for {symbol} to {sl_price}")
        result = execute_shark_order(side="SELL", symbol=symbol, quantity=quantity, stop_loss_price=sl_price, reduce_only=True)

    else:
        return {"status": "ignored", "reason": f"Unknown action: {action}"}

    return {"status": "success", "shark_response": result}
