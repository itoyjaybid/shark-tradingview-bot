import os
import time
import json
import hmac
import hashlib
import requests
import threading
from fastapi import FastAPI, Request, HTTPException

SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

DEFAULT_SYMBOL = "BTCUSDT"
STATE_FILE = "state.json"
ORDER_LOCK = threading.Lock()

app = FastAPI()

# =============================================================================
# PERSISTENCE
# =============================================================================
def get_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"trade_id": None, "side": None, "qty": 0.0, "sl_id": None, "entry_id": None}

def set_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"[STATE ERROR] {e}", flush=True)

# =============================================================================
# EXACT SERVER TIME & SIGNATURE
# =============================================================================
def get_server_time():
    """Pulls exact current timestamp directly from Shark Exchange to prevent 4007 errors."""
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=2)
        if r.status_code == 200:
            data = r.json()
            ts = data.get("serverTime") or data.get("data")
            if ts:
                return int(ts)
    except Exception:
        pass
    return int(time.time() * 1000)

def sign_payload(payload_str: str) -> dict:
    sig = hmac.new(
        SHARK_API_SECRET.encode("utf-8"),
        payload_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return {
        "api-key": SHARK_API_KEY,
        "signature": sig,
        "Content-Type": "application/json"
    }

def clean_symbol(sym: str) -> str:
    if not sym:
        return DEFAULT_SYMBOL
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()

# =============================================================================
# EXCHANGE CALLS
# =============================================================================
def delete_order(client_order_id: str):
    if not client_order_id:
        return
    ts = get_server_time()
    payload = {"clientOrderId": str(client_order_id), "timestamp": ts}
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_payload(body)
    try:
        r = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=3)
        print(f"[CANCEL] Order {client_order_id} -> HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)

def place_market_close(symbol: str, side: str, qty: float):
    ts = get_server_time()
    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": float(f"{qty:.4f}"),
        "reduceOnly": True,
        "side": side,
        "symbol": clean_symbol(symbol),
        "timestamp": ts,
        "type": "MARKET",
        "userCategory": "EXTERNAL"
    }
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_payload(body)
    try:
        r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=3)
        print(f"[FLATTEN] {side} {qty} -> HTTP {r.status_code}: {r.text}", flush=True)
    except Exception as e:
        print(f"[FLATTEN ERROR] {e}", flush=True)

def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> str:
    ts = get_server_time()
    offset = 15.0
    limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
    
    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "price": float(f"{limit_price:.2f}"),
        "quantity": float(f"{qty:.4f}"),
        "reduceOnly": True,
        "side": side,
        "stopPrice": float(f"{stop_price:.2f}"),
        "symbol": clean_symbol(symbol),
        "timestamp": ts,
        "type": "STOP_LIMIT",
        "userCategory": "EXTERNAL"
    }
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_payload(body)
    try:
        print(f"[STOP PLACE] Submitting {side} Stop @ {stop_price} (Limit: {limit_price})...", flush=True)
        r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=3)
        if r.status_code in [200, 201]:
            res = r.json()
            cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
            print(f"[STOP SUCCESS] Active SL ID: {cid}", flush=True)
            return cid
        else:
            print(f"[STOP ERROR] HTTP {r.status_code}: {r.text}", flush=True)
    except Exception as e:
        print(f"[STOP EXCEPTION] {e}", flush=True)
    return None

def check_fill(client_order_id: str, symbol: str) -> tuple[bool, float]:
    target = clean_symbol(symbol)
    ts = get_server_time()
    query = f"clientOrderId={client_order_id}&symbol={target}&timestamp={ts}"
    headers = sign_payload(query)
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{query}", headers=headers, timeout=2)
        if r.status_code == 200:
            data = r.json()
            order = data.get("data", data)
            if isinstance(order, dict):
                status = str(order.get("status", "")).upper()
                p = float(order.get("avgPrice") or order.get("executedPrice") or 0.0)
                if status in ["FILLED", "SUCCESS", "EXECUTED"]:
                    return True, p
    except Exception:
        pass
    return False, 0.0

# =============================================================================
# WATCHER THREAD FOR ENTRY FILL
# =============================================================================
def monitor_fill_and_set_sl(symbol: str, side: str, entry_price: float, trade_id: int, qty: float, order_id: str):
    is_buy = side == "BUY"
    sl_side = "SELL" if is_buy else "BUY"
    target = clean_symbol(symbol)

    print(f"[WATCHER] Waiting for entry {order_id} to fill...", flush=True)
    for _ in range(300):
        time.sleep(1.0)
        state = get_state()
        if state.get("trade_id") != trade_id:
            return

        filled, exec_p = check_fill(order_id, target)
        if filled:
            real_price = exec_p if exec_p > 0 else entry_price
            initial_stop = round(real_price - 100.0, 2) if is_buy else round(real_price + 100.0, 2)
            print(f"[ENTRY FILLED] Price: {real_price}. Placing initial Stop @ {initial_stop}...", flush=True)
            
            sl_id = place_stop_order(target, sl_side, qty, initial_stop)
            state["side"] = side
            state["qty"] = qty
            state["sl_id"] = sl_id
            set_state(state)
            return

    print(f"[WATCHER TIMEOUT] Order {order_id} did not fill. Canceling...", flush=True)
    delete_order(order_id)
    state = get_state()
    if state.get("entry_id") == order_id:
        state["entry_id"] = None
        set_state(state)

# =============================================================================
# WORKFLOW EXECUTION
# =============================================================================
def process_entry(action: str, symbol: str, qty: float, price: float, trade_id: int):
    with ORDER_LOCK:
        target = clean_symbol(symbol)
        side = "BUY" if "BUY" in action.upper() else "SELL"
        state = get_state()

        # 1. Reverse/Close previous position if active
        if state.get("side") and state.get("qty", 0.0) > 0:
            close_side = "SELL" if state["side"] == "BUY" else "BUY"
            if state.get("sl_id"):
                delete_order(state["sl_id"])
            place_market_close(target, close_side, state["qty"])

        # 2. Reset state for new trade
        state = {
            "trade_id": trade_id,
            "side": None,
            "qty": qty,
            "sl_id": None,
            "entry_id": None
        }
        set_state(state)

        # 3. Post LIMIT entry
        ts = get_server_time()
        payload = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": float(f"{price:.2f}"),
            "quantity": float(f"{qty:.4f}"),
            "reduceOnly": False,
            "side": side,
            "symbol": target,
            "timestamp": ts,
            "type": "LIMIT",
            "userCategory": "EXTERNAL"
        }
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        headers = sign_payload(body)

        try:
            print(f"[ENTRY SUBMIT] Placing {side} {qty} @ {price}...", flush=True)
            r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=3)
            if r.status_code in [200, 201]:
                res = r.json()
                cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
                print(f"[ENTRY SUCCESS] Order ID: {cid}", flush=True)
                state["entry_id"] = cid
                set_state(state)
                threading.Thread(
                    target=monitor_fill_and_set_sl,
                    args=(target, side, price, trade_id, qty, cid),
                    daemon=True
                ).start()
            else:
                print(f"[ENTRY ERROR] HTTP {r.status_code}: {r.text}", flush=True)
        except Exception as e:
            print(f"[ENTRY EXCEPTION] {e}", flush=True)

def process_trailing_sl(symbol: str, qty: float, sl_price: float, trade_id: int):
    with ORDER_LOCK:
        state = get_state()
        if state.get("trade_id") != trade_id or not state.get("side"):
            print(f"[TRAIL IGNORED] No active trade {trade_id} in state.", flush=True)
            return

        target = clean_symbol(symbol)
        sl_side = "SELL" if state["side"] == "BUY" else "BUY"

        # Cancel previous stop first to avoid exceeding position limit
        if state.get("sl_id"):
            delete_order(state["sl_id"])
            state["sl_id"] = None

        new_sl = place_stop_order(target, sl_side, qty, sl_price)
        if new_sl:
            state["sl_id"] = new_sl
            set_state(state)
            print(f"[TRAIL UPDATED] Stop is now at {sl_price} (ID: {new_sl})", flush=True)

# =============================================================================
# WEBHOOK ROUTE
# =============================================================================
@app.api_route("/", methods=["GET", "HEAD"])
async def ping():
    return {"status": "ok"}

@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        return {"status": "bad json"}

    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Forbidden")

    action = str(data.get("action", "")).upper()
    symbol = str(data.get("symbol", DEFAULT_SYMBOL))
    qty = float(data.get("quantity", 0.002))
    price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))

    if action == "UPDATE_SL" and sl_price == 0.0 and price > 0.0:
        sl_price = price

    print(f"\n[ALERT] {action} | TradeID: {trade_id} | Price: {price} | SL: {sl_price}", flush=True)

    if action in ["BUY", "SELL"]:
        threading.Thread(target=process_entry, args=(action, symbol, qty, price, trade_id), daemon=True).start()
    elif action == "UPDATE_SL":
        threading.Thread(target=process_trailing_sl, args=(symbol, qty, sl_price, trade_id), daemon=True).start()

    return {"status": "received"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 10000)))
