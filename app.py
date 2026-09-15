import os
import time
import json
import hmac
import hashlib
import socket
import requests
import threading
from contextlib import asynccontextmanager
import urllib3.util.connection as urllib3_cn
from fastapi import FastAPI, Request, HTTPException

# Force IPv4 routing on cloud hosts
def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

# =============================================================================
# CONFIGURATION
# =============================================================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

DEFAULT_SYMBOL = "BTCUSDT"
STATE_FILE = "state.json"
ENTRY_TIMEOUT_SECONDS = 300

ENGINE_LOCK = threading.Lock()

# Persistent State Cache
STATE = {
    "trade_id": None,
    "side": None,           # "BUY" or "SELL"
    "qty": 0.0,
    "entry_id": None,
    "sl_id": None,
    "fill_price": 0.0,
    "sl_price": 0.0
}

# Real-Time Monotonic Clock Sync
CLOCK_OFFSET_MS = 0


# =============================================================================
# PERSISTENCE HANDLERS
# =============================================================================
def load_state():
    global STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                STATE.update(data)
                print(f"[STATE LOADED] Active Trade: {STATE['trade_id']} | Side: {STATE['side']} | Qty: {STATE['qty']}", flush=True)
        except Exception as e:
            print(f"[STATE READ ERROR]: {e}", flush=True)


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(STATE, f, indent=2)
    except Exception as e:
        print(f"[STATE WRITE ERROR]: {e}", flush=True)


# =============================================================================
# CONTINUOUS TIME SYNCHRONIZATION ENGINE
# =============================================================================
def time_sync_worker():
    """Background daemon continually keeping millisecond offset aligned with exchange."""
    global CLOCK_OFFSET_MS
    while True:
        try:
            r = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=2)
            if r.status_code == 200:
                res = r.json()
                srv_ts = int(res.get("serverTime") or res.get("data") or 0)
                if srv_ts > 0:
                    local_ts = int(time.time() * 1000)
                    CLOCK_OFFSET_MS = srv_ts - local_ts
        except Exception:
            pass
        time.sleep(15)


def get_synced_time() -> int:
    return int(time.time() * 1000) + CLOCK_OFFSET_MS


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    # Initial blocking sync
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=2)
        if r.status_code == 200:
            res = r.json()
            srv = int(res.get("serverTime") or res.get("data") or 0)
            if srv > 0:
                global CLOCK_OFFSET_MS
                CLOCK_OFFSET_MS = srv - int(time.time() * 1000)
                print(f"[INITIAL CLOCK SYNC] Offset: {CLOCK_OFFSET_MS}ms", flush=True)
    except Exception:
        pass
    threading.Thread(target=time_sync_worker, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


# =============================================================================
# SIGNING & EXCHANGE HTTP UTILITIES
# =============================================================================
def sign_data(data_str: str) -> dict:
    sig = hmac.new(
        SHARK_API_SECRET.encode("utf-8"),
        data_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return {
        "api-key": SHARK_API_KEY,
        "signature": sig,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }


def clean_symbol(sym: str) -> str:
    if not sym:
        return DEFAULT_SYMBOL
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()


def send_order_request(payload: dict) -> tuple[bool, str, dict]:
    """Submits order with native int timestamp and auto-resyncs on edge 4007 errors."""
    payload["timestamp"] = get_synced_time()
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_data(body)

    try:
        r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=3)
        if r.status_code in [200, 201]:
            res = r.json()
            cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
            return True, cid, res
        
        # Immediate fallback retry if cloud latency caused signature shift
        if r.status_code == 403 or "Signature mismatch" in r.text:
            payload["timestamp"] = get_synced_time()
            body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            headers = sign_data(body)
            r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=3)
            if r.status_code in [200, 201]:
                res = r.json()
                cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
                return True, cid, res

        print(f"[ORDER REJECTED] HTTP {r.status_code}: {r.text}", flush=True)
        return False, None, {}
    except Exception as e:
        print(f"[ORDER SEND EXCEPTION]: {e}", flush=True)
        return False, None, {}


def cancel_exchange_order(client_order_id: str) -> bool:
    """Cancels order using minimal JSON schema (native int timestamp, no symbol)."""
    if not client_order_id:
        return True
    payload = {"clientOrderId": str(client_order_id), "timestamp": get_synced_time()}
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_data(body)

    try:
        r = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=3)
        print(f"[CLEANUP] Cancel {client_order_id} -> HTTP {r.status_code}", flush=True)
        return r.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP EXCEPTION]: {e}", flush=True)
        return False


# =============================================================================
# ORDER STATUS & FILL DETECTION
# =============================================================================
def is_in_open_orders(client_order_id: str, symbol: str) -> bool:
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"symbol={target}&timestamp={ts}"
    headers = sign_data(query)

    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/order/open-orders?{query}", headers=headers, timeout=2)
        if r.status_code == 200:
            data = r.json()
            orders = data.get("data", data)
            if isinstance(orders, list):
                for o in orders:
                    cid = str(o.get("clientOrderId") or o.get("orderId") or "")
                    if client_order_id in cid or cid in client_order_id:
                        return True
    except Exception:
        pass
    return False


def get_executed_price(client_order_id: str, symbol: str) -> tuple[str, float]:
    """Returns exact status string and average executed fill price from exchange."""
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"clientOrderId={client_order_id}&symbol={target}&timestamp={ts}"
    headers = sign_data(query)

    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{query}", headers=headers, timeout=2)
        if r.status_code == 200:
            data = r.json()
            order = data.get("data", data)
            if isinstance(order, dict):
                st = str(order.get("status") or "").upper()
                p = float(order.get("avgPrice") or order.get("executedPrice") or order.get("price") or 0.0)
                return st, p
    except Exception:
        pass
    return "UNKNOWN", 0.0


# =============================================================================
# CORE EXECUTION ROUTINES
# =============================================================================
def place_stop_order(symbol: str, side: str, qty: float, stop_price: float) -> str:
    target = clean_symbol(symbol)
    offset = 15.0
    limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
    clean_stop = float(f"{stop_price:.2f}")
    clean_limit = float(f"{limit_price:.2f}")
    clean_qty = float(f"{qty:.4f}")

    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "price": clean_limit,
        "quantity": clean_qty,
        "reduceOnly": True,
        "side": side,
        "stopPrice": clean_stop,
        "symbol": target,
        "type": "STOP_LIMIT",
        "userCategory": "EXTERNAL"
    }

    print(f"[STOP PLACE] Submitting {side} Stop @ {clean_stop} (Limit: {clean_limit})...", flush=True)
    ok, cid, _ = send_order_request(payload)
    if ok and cid:
        print(f">>> [STOP CONFIRMED] Placed Stop ID: {cid}", flush=True)
        return cid
    return None


def execute_market_flatten(symbol: str, side: str, qty: float):
    target = clean_symbol(symbol)
    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": float(f"{qty:.4f}"),
        "reduceOnly": True,
        "side": side,
        "symbol": target,
        "type": "MARKET",
        "userCategory": "EXTERNAL"
    }
    print(f"[REVERSAL FLATTEN] Liquidating {side} {qty}...", flush=True)
    ok, cid, _ = send_order_request(payload)
    if ok:
        print(f">>> [REVERSAL SUCCESS] Prior position closed.", flush=True)


def entry_watcher(symbol: str, side: str, signal_price: float, trade_id: int, qty: float, order_id: str):
    target = clean_symbol(symbol)
    is_buy = side == "BUY"
    sl_side = "SELL" if is_buy else "BUY"
    start_time = time.time()

    print(f"[WATCHER] Monitoring order {order_id} for fill...", flush=True)
    time.sleep(1.0)

    while (time.time() - start_time) < ENTRY_TIMEOUT_SECONDS:
        time.sleep(0.8)

        if STATE.get("trade_id") != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting.", flush=True)
            return

        st, p = get_executed_price(order_id, target)
        still_open = is_in_open_orders(order_id, target)

        # Matched if order reports filled status OR is completely off the open book
        if st in ["FILLED", "SUCCESS", "EXECUTED"] or (not still_open and st != "CANCELED"):
            print(f">>> [FILL CONFIRMED] Order {order_id} filled!", flush=True)

            # Micro-poll up to 1 second to pull executed average price if delayed
            real_fill = p
            if real_fill <= 0:
                for _ in range(4):
                    time.sleep(0.25)
                    _, retry_p = get_executed_price(order_id, target)
                    if retry_p > 0:
                        real_fill = retry_p
                        break

            if real_fill <= 0:
                real_fill = signal_price

            print(f">>> [FILL PRICE ACCURATE]: {real_fill}", flush=True)

            with ENGINE_LOCK:
                STATE["side"] = side
                STATE["qty"] = qty
                STATE["fill_price"] = real_fill
                STATE["entry_id"] = None
                save_state()

                # Place initial 100 pt Stop Loss
                initial_stop = round(real_fill - 100.0, 2) if is_buy else round(real_fill + 100.0, 2)
                sl_id = place_stop_order(target, sl_side, qty, initial_stop)
                if sl_id:
                    STATE["sl_id"] = sl_id
                    STATE["sl_price"] = initial_stop
                    save_state()
            return

    print(f"[WATCHER TIMEOUT] Order {order_id} unfilled after {ENTRY_TIMEOUT_SECONDS}s. Canceling...", flush=True)
    with ENGINE_LOCK:
        cancel_exchange_order(order_id)
        if STATE.get("entry_id") == order_id:
            STATE["entry_id"] = None
            save_state()


def handle_entry_signal(action: str, symbol: str, qty: float, price: float, trade_id: int):
    with ENGINE_LOCK:
        target = clean_symbol(symbol)
        side = "BUY" if "BUY" in action.upper() else "SELL"

        # 1. Clean up any active resting entry or active stop
        if STATE.get("entry_id"):
            cancel_exchange_order(STATE["entry_id"])
            STATE["entry_id"] = None

        if STATE.get("sl_id"):
            cancel_exchange_order(STATE["sl_id"])
            STATE["sl_id"] = None

        # 2. Reverse existing open trade if opposite side
        if STATE.get("side") and STATE.get("qty", 0.0) > 0:
            close_side = "SELL" if STATE["side"] == "BUY" else "BUY"
            execute_market_flatten(target, close_side, STATE["qty"])

        # 3. Setup new trade state
        STATE["trade_id"] = trade_id
        STATE["side"] = side
        STATE["qty"] = qty
        STATE["entry_id"] = None
        STATE["sl_id"] = None
        STATE["fill_price"] = price
        STATE["sl_price"] = 0.0
        save_state()

        # 4. Post LIMIT Entry
        clean_p = float(f"{price:.2f}")
        clean_q = float(f"{qty:.4f}")
        payload = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": clean_p,
            "quantity": clean_q,
            "reduceOnly": False,
            "side": side,
            "symbol": target,
            "type": "LIMIT",
            "userCategory": "EXTERNAL"
        }

        print(f"\n[ENTRY SUBMIT] Placing {side} {clean_q} ({target}) @ Limit {clean_p}...", flush=True)
        ok, cid, _ = send_order_request(payload)
        if ok and cid:
            print(f">>> [SUCCESS] Limit Entry Posted. ID: {cid}", flush=True)
            STATE["entry_id"] = cid
            save_state()

            threading.Thread(
                target=entry_watcher,
                args=(target, side, clean_p, trade_id, clean_q, cid),
                daemon=True
            ).start()


def handle_trailing_signal(symbol: str, qty: float, sl_price: float, trade_id: int):
    """Safely updates the trailing stop by freeing reduceOnly limits first, with instant fallback."""
    with ENGINE_LOCK:
        target = clean_symbol(symbol)

        if STATE.get("trade_id") != trade_id or not STATE.get("side"):
            print(f"[UPDATE_SL IGNORED] No active trade {trade_id} found in state. Discarding.", flush=True)
            return

        new_stop = float(f"{sl_price:.2f}")
        if new_stop <= 0:
            return

        pos_side = STATE["side"]
        sl_side = "SELL" if pos_side == "BUY" else "BUY"
        old_sl_id = STATE.get("sl_id")
        old_sl_price = STATE.get("sl_price", 0.0)

        print(f"[UPDATE_SL] Updating {sl_side} Stop from {old_sl_price} to {new_stop}...", flush=True)

        # Step 1: Cancel old stop to free the reduceOnly allocation
        if old_sl_id:
            cancel_exchange_order(old_sl_id)
            STATE["sl_id"] = None
            save_state()

        # Step 2: Post the tighter stop
        new_sl_id = place_stop_order(target, sl_side, qty, new_stop)

        if new_sl_id:
            STATE["sl_id"] = new_sl_id
            STATE["sl_price"] = new_stop
            save_state()
            print(f">>> [TRAILING SL SUCCESS] Stop moved to {new_stop} | ID: {new_sl_id}", flush=True)
        else:
            # Fallback: re-establish protective stop immediately if update was rejected
            print(f"[UPDATE_SL ALERT] Placement rejected! Restoring safety stop @ {old_sl_price}...", flush=True)
            if old_sl_price > 0:
                restored_id = place_stop_order(target, sl_side, qty, old_sl_price)
                if restored_id:
                    STATE["sl_id"] = restored_id
                    save_state()


# =============================================================================
# WEBHOOK ENDPOINTS
# =============================================================================
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"service": "Shark Trading Engine", "status": "online"}


@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        data = await request.json()
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

    print(f"\n[ALERT] Action: {action} | TradeID: {trade_id} | Price: {price} | SL: {sl_price}", flush=True)

    if action in ["BUY", "SELL"]:
        threading.Thread(
            target=handle_entry_signal,
            args=(action, symbol, qty, price, trade_id),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=handle_trailing_signal,
            args=(symbol, qty, sl_price, trade_id),
            daemon=True
        ).start()

    return {"status": "accepted"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
