from fastapi import FastAPI, Request, HTTPException
import os
import time
import json
import hmac
import hashlib
import socket
import requests
import threading

# Force IPv4 for all outbound requests on cloud hosts (Render/Railway)
import urllib3.util.connection as urllib3_cn

def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

app = FastAPI()

# ==========================================
# SHARK EXCHANGE CONFIGURATION
# ==========================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip()
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip()

CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []


def generate_signature(secret: str, data: str) -> str:
    return hmac.new(secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def get_headers(payload_str: str) -> dict:
    return {
        "api-key": SHARK_API_KEY,
        "signature": generate_signature(SHARK_API_SECRET, payload_str),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }


def delete_single_order(client_order_id: str) -> bool:
    """Deletes an order using the payload schema required by Shark Exchange."""
    try:
        ts = str(int(time.time() * 1000))
        payload = {
            "timestamp": ts,
            "clientOrderId": str(client_order_id)
        }
        body = json.dumps(payload, separators=(",", ":"))
        headers = get_headers(body)

        resp = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=5)
        print(f"[CLEANUP] Deleted SL ({client_order_id}) -> HTTP {resp.status_code}: {resp.text}")
        return resp.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}")
        return False


def cancel_all_tracked_stops():
    """Iterates through and cancels all active resting Stop Loss orders."""
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return

    print(f"[CLEANUP] Deleting active SL clientOrderIds: {ACTIVE_SL_CLIENT_IDS}")
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid)
    ACTIVE_SL_CLIENT_IDS = []


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0):
    """Places STOP_MARKET with Error 3011 pre-flight check."""
    global ACTIVE_SL_CLIENT_IDS
    try:
        # Pre-flight check: Prevent submitting stops that activate instantly (Error 3011 guard)
        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[REJECTED 3011 GUARD] Long SL ({stop_price}) >= Market Price ({ref_price}). Skipping.")
                return
            if side == "BUY" and stop_price <= ref_price:
                print(f"[REJECTED 3011 GUARD] Short SL ({stop_price}) <= Market Price ({ref_price}). Skipping.")
                return

        time.sleep(0.1)
        sl_timestamp = str(int(time.time() * 1000))

        sl_params = {
            "timestamp": sl_timestamp,
            "placeType": "ORDER_FORM",
            "quantity": quantity,
            "side": side,
            "symbol": symbol,
            "type": "STOP_MARKET",
            "reduceOnly": True,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL",
            "stopPrice": stop_price
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"))
        sl_headers = get_headers(sl_body)

        print(f"[STOP LOSS] Submitting {side} STOP_MARKET for {quantity} {symbol} at {stop_price}...")
        resp = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=sl_body, headers=sl_headers, timeout=5)

        if resp.status_code in [200, 201]:
            res_data = resp.json()
            cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
            if cid:
                ACTIVE_SL_CLIENT_IDS.append(cid)
                print(f">>> [SUCCESS] Trailing SL Placed! Active clientOrderId: {cid}")
            else:
                print(f">>> [SUCCESS] Response: {res_data}")
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
    except Exception as e:
        print(f"[SL EXECUTION ERROR]: {e}")


def execute_entry_order(action: str, symbol: str, quantity: float, sl_price: float = 0.0, current_price: float = 0.0):
    """Executes Market Entry, cleans old stops, and attaches initial SL."""
    global CURRENT_POSITION_SIDE
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
        side = action.upper()
        raw_qty = float(quantity)
        order_qty = int(raw_qty) if raw_qty.is_integer() else round(raw_qty, 4)

        raw_sl = float(sl_price) if sl_price else 0.0
        stop_price = int(raw_sl) if raw_sl.is_integer() else round(raw_sl, 2)
        ref_price = float(current_price) if current_price else 0.0

        # Clear prior resting stops before placing new entry
        cancel_all_tracked_stops()

        timestamp = str(int(time.time() * 1000))
        entry_params = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": order_qty,
            "side": side,
            "symbol": clean_symbol,
            "type": "MARKET",
            "reduceOnly": False,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL"
        }

        entry_body = json.dumps(entry_params, separators=(",", ":"))
        entry_headers = get_headers(entry_body)

        print(f"\n[ENTRY] Placing {side} {order_qty} {clean_symbol}...")
        resp_entry = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=entry_body, headers=entry_headers, timeout=5)

        if resp_entry.status_code in [200, 201]:
            CURRENT_POSITION_SIDE = side
            print(f">>> [SUCCESS] Entry Filled! Response: {resp_entry.json()}")
        else:
            print(f">>> [ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")
            return

        if stop_price > 0:
            sl_side = "SELL" if side == "BUY" else "BUY"
            place_stop_loss(clean_symbol, sl_side, order_qty, stop_price, ref_price)

    except Exception as e:
        print(f"[BRIDGE EXECUTION ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float):
    """Deletes old stop and places updated trailing stop."""
    global CURRENT_POSITION_SIDE
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
        raw_qty = float(quantity)
        order_qty = int(raw_qty) if raw_qty.is_integer() else round(raw_qty, 4)

        raw_sl = float(sl_price) if sl_price else 0.0
        stop_price = int(raw_sl) if raw_sl.is_integer() else round(raw_sl, 2)
        ref_price = float(current_price) if current_price else 0.0

        if stop_price > 0:
            cancel_all_tracked_stops()

            if CURRENT_POSITION_SIDE == "SELL":
                sl_side = "BUY"
            elif CURRENT_POSITION_SIDE == "BUY":
                sl_side = "SELL"
            else:
                sl_side = "BUY" if (ref_price > 0 and stop_price > ref_price) else "SELL"

            place_stop_loss(clean_symbol, sl_side, order_qty, stop_price, ref_price)

    except Exception as e:
        print(f"[TRAILING SL ERROR]: {e}")


# ==========================================
# FASTAPI ROUTES
# ==========================================
@app.get("/")
def home():
    return {"status": "awake", "service": "Shark Trading Bot"}


@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"status": "ignored_empty"}

    # 1. Verify passphrase
    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Invalid secret passphrase")

    action = str(data.get("action", "")).upper()
    symbol = str(data.get("symbol", "BTCUSDT"))
    quantity = float(data.get("quantity", 0.002))
    sl_price = float(data.get("sl_price", 0.0))
    current_price = float(data.get("price", 0.0))

    print(f"\n[ALERT RECEIVED] Action: {action} | Symbol: {symbol} | Qty: {quantity} | SL: {sl_price} | Price: {current_price}")

    # 2. Dispatch background thread
    if action in ["BUY", "SELL"]:
        threading.Thread(
            target=execute_entry_order,
            args=(action, symbol, quantity, sl_price, current_price),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=update_trailing_stop,
            args=(symbol, quantity, sl_price, current_price),
            daemon=True
        ).start()

    elif action in ["SL_EXIT", "EXIT", "CLOSE"]:
        threading.Thread(
            target=cancel_all_tracked_stops,
            daemon=True
        ).start()

    else:
        return {"status": "ignored", "reason": f"Unknown action: {action}"}

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
