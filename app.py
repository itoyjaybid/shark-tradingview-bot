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
# ENVIRONMENT & CONFIGURATION
# =============================================================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

DEFAULT_SYMBOL = "BTCUSDT"
STATE_FILE = "state.json"

ENGINE_LOCK = threading.Lock()
TS_LOCK = threading.Lock()

LAST_USED_TIMESTAMP = 0
CLOCK_OFFSET_MS = 0

BOT_STATE = {
    "trade_id": None,
    "side": None,           # "BUY" or "SELL"
    "qty": 0.0,
    "entry_id": None,
    "sl_id": None,
    "fill_price": 0.0,
    "sl_price": 0.0
}

# =============================================================================
# STATE PERSISTENCE
# =============================================================================
def load_state():
    global BOT_STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                BOT_STATE.update(json.load(f))
                print(f"[STATE LOADED] TradeID: {BOT_STATE['trade_id']} | Side: {BOT_STATE['side']} | Qty: {BOT_STATE['qty']}", flush=True)
        except Exception as e:
            print(f"[STATE LOAD ERROR]: {e}", flush=True)

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(BOT_STATE, f, indent=2)
    except Exception as e:
        print(f"[STATE SAVE ERROR]: {e}", flush=True)

def purge_state():
    BOT_STATE["trade_id"] = None
    BOT_STATE["side"] = None
    BOT_STATE["qty"] = 0.0
    BOT_STATE["entry_id"] = None
    BOT_STATE["sl_id"] = None
    BOT_STATE["fill_price"] = 0.0
    BOT_STATE["sl_price"] = 0.0
    save_state()

# =============================================================================
# CLOCK SYNCHRONIZATION
# =============================================================================
def sync_clock_directly() -> int:
    """Synchronizes offset directly against Shark Exchange server time."""
    global CLOCK_OFFSET_MS
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=2)
        if r.status_code == 200:
            res = r.json()
            srv_ts = int(res.get("serverTime") or res.get("data") or 0)
            if srv_ts > 0:
                CLOCK_OFFSET_MS = srv_ts - int(time.time() * 1000)
                return srv_ts
    except Exception:
        pass
    return int(time.time() * 1000) + CLOCK_OFFSET_MS

def get_synced_time() -> int:
    global LAST_USED_TIMESTAMP, CLOCK_OFFSET_MS
    with TS_LOCK:
        now_ts = int(time.time() * 1000) + CLOCK_OFFSET_MS
        if now_ts <= LAST_USED_TIMESTAMP:
            now_ts = LAST_USED_TIMESTAMP + 1
        LAST_USED_TIMESTAMP = now_ts
        return now_ts

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    sync_clock_directly()
    print(f"[BOOT] Initial Clock Offset: {CLOCK_OFFSET_MS}ms", flush=True)
    yield

app = FastAPI(lifespan=lifespan)

# =============================================================================
# SIGNING & HTTP DISPATCH WITH VERBOSE LOGGING
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
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace(".p", "").replace("/", "").upper()

def send_signed_order(payload: dict, max_retries: int = 3) -> tuple[bool, str]:
    for attempt in range(max_retries):
        if attempt > 0:
            time.sleep(0.15)
            sync_clock_directly()

        payload["timestamp"] = get_synced_time()
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        headers = sign_data(body)

        try:
            r = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body, headers=headers, timeout=4)
            print(f"[EXCHANGE RESPONSE] Status: {r.status_code} | Body: {r.text}", flush=True)

            if r.status_code in [200, 201]:
                res = r.json()
                cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
                return True, cid

            if "4007" in r.text or "Signature" in r.text or r.status_code == 403:
                print(f"[SIGNATURE RETRY] Attempt {attempt + 1} failed. Resyncing clock...", flush=True)
                continue

            return False, None

        except Exception as e:
            print(f"[DISPATCH ERROR]: {e}", flush=True)
            time.sleep(0.1)

    return False, None

def cancel_order(client_order_id: str) -> bool:
    if not client_order_id:
        return True
    payload = {"clientOrderId": str(client_order_id), "timestamp": get_synced_time()}
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_data(body)
    try:
        r = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=3)
        return r.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}", flush=True)
        return False

# =============================================================================
# EXCHANGE AUDIT & TICKER
# =============================================================================
def get_exchange_position_state(symbol: str) -> tuple[float, str]:
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    
    endpoints = [
        f"/v1/position/open-positions?symbol={target}&timestamp={ts}",
        f"/v1/positions?symbol={target}&timestamp={ts}"
    ]

    for ep in endpoints:
        try:
            querystr = ep.split("?")[1]
            headers = sign_data(querystr)
            r = requests.get(f"{SHARK_BASE_URL}{ep}", headers=headers, timeout=2)
            if r.status_code == 200:
                res = r.json()
                data = res.get("data", res)
                items = data if isinstance(data, list) else [data]
                for pos in items:
                    sym = str(pos.get("symbol", "")).upper()
                    if target in sym or sym in target:
                        raw_qty = float(pos.get("positionAmt") or pos.get("size") or pos.get("positionAmount") or 0.0)
                        side_str = str(pos.get("side") or pos.get("positionSide") or "").upper()
                        
                        if abs(raw_qty) > 0.00001:
                            if side_str in ["SHORT", "SELL"] or raw_qty < 0:
                                return abs(raw_qty), "SELL"
                            return abs(raw_qty), "BUY"
                        return 0.0, "FLAT"
        except Exception:
            continue

    return 0.0, "FLAT"

def get_current_ticker_price(symbol: str) -> float:
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"symbol={target}&timestamp={ts}"
    headers = sign_data(query)
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/ticker/price?{query}", headers=headers, timeout=2)
        if r.status_code == 200:
            res = r.json()
            data = res.get("data", res)
            return float(data.get("price") or data.get("lastPrice") or 0.0)
    except Exception:
        pass
    return 0.0

# =============================================================================
# FLASH CRASH & SLIPPAGE SWEEPER
# =============================================================================
def monitor_slippage_and_spike_fallback(symbol: str, sl_client_id: str, limit_price: float, side: str, qty: float, trade_id: int):
    target = clean_symbol(symbol)
    is_buy_back = side == "BUY"

    time.sleep(1.0)
    while BOT_STATE.get("sl_id") == sl_client_id and BOT_STATE.get("trade_id") == trade_id:
        time.sleep(0.5)
        curr_price = get_current_ticker_price(target)
        if curr_price <= 0.0:
            continue

        gapped = (is_buy_back and curr_price > limit_price) or ((not is_buy_back) and curr_price < limit_price)
        if gapped:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit buffer ({limit_price})!", flush=True)
            with ENGINE_LOCK:
                if BOT_STATE.get("sl_id") == sl_client_id:
                    print("[EMERGENCY FALLBACK] Liquidating via MARKET order...", flush=True)
                    cancel_order(sl_client_id)
                    flatten_position(target, side, qty)
                    purge_state()
            break

# =============================================================================
# ORDER OPERATIONS
# =============================================================================
def place_stop_loss(symbol: str, side: str, qty: float, stop_price: float, trade_id: int) -> str:
    target = clean_symbol(symbol)
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
        "symbol": target,
        "type": "STOP_LIMIT",
        "userCategory": "EXTERNAL"
    }

    print(f"[STOP PLACE] Submitting {side} Stop @ {payload['stopPrice']} (Limit Buffer: {payload['price']})...", flush=True)
    ok, cid = send_signed_order(payload)
    if ok and cid:
        print(f">>> [STOP PLACED] Order ID: {cid}", flush=True)
        threading.Thread(
            target=monitor_slippage_and_spike_fallback,
            args=(target, cid, payload["price"], side, payload["quantity"], trade_id),
            daemon=True
        ).start()
        return cid
    return None

def flatten_position(symbol: str, side: str, qty: float):
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
    print(f"[IMMEDIATE EXIT] Market liquidation {side} {qty}...", flush=True)
    send_signed_order(payload)

# =============================================================================
# HYBRID FILL WATCHER (ORDER DETAIL + ACTIVE POSITION BACKSTOP)
# =============================================================================
def check_fill_status(client_order_id: str, symbol: str, target_side: str, target_qty: float) -> tuple[bool, float]:
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"clientOrderId={client_order_id}&symbol={target}&timestamp={ts}"
    headers = sign_data(query)

    # 1. Order Detail API Check
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{query}", headers=headers, timeout=2)
        if r.status_code == 200:
            res_json = r.json()
            order = res_json.get("data", res_json)
            if isinstance(order, dict):
                if "order" in order and isinstance(order["order"], dict):
                    order = order["order"]
                st = str(order.get("status") or order.get("orderStatus") or order.get("state") or "").upper()
                exec_qty = float(order.get("executedQty") or order.get("cumQty") or order.get("filledQty") or 0.0)
                p = float(order.get("avgPrice") or order.get("executedPrice") or order.get("price") or 0.0)

                if st in ["FILLED", "SUCCESS", "EXECUTED", "COMPLETE"] or exec_qty >= (target_qty * 0.90):
                    return True, p
                if st in ["CANCELED", "CANCELLED", "REJECTED", "EXPIRED"]:
                    return False, -1.0
    except Exception as e:
        print(f"[STATUS CHECK ERROR]: {e}", flush=True)

    # 2. Live Position Backstop Check
    live_qty, live_side = get_exchange_position_state(target)
    if live_qty >= (target_qty * 0.90) and live_side == target_side:
        curr_p = get_current_ticker_price(target)
        print(f">>> [LIVE POSITION DETECTED] Shark Exchange confirms active position: {live_side} {live_qty}", flush=True)
        return True, curr_p

    return False, 0.0

def entry_order_watcher(symbol: str, side: str, limit_price: float, trade_id: int, qty: float, order_id: str, timeout_sec: int):
    target = clean_symbol(symbol)
    sl_side = "SELL" if side == "BUY" else "BUY"
    start_time = time.time()

    print(f"[WATCHER] Monitoring limit entry {order_id} (Timeout: {timeout_sec}s)...", flush=True)
    time.sleep(1.0)

    while (time.time() - start_time) < timeout_sec:
        time.sleep(0.8)

        if BOT_STATE.get("trade_id") != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting.", flush=True)
            return

        filled, exec_p = check_fill_status(order_id, target, side, qty)
        if exec_p == -1.0:
            print(f"[WATCHER] Order {order_id} canceled or rejected by exchange. Exiting.", flush=True)
            return

        if filled:
            real_fill = exec_p if exec_p > 0 else limit_price
            print(f">>> [FILL CONFIRMED] Entry executed @ {real_fill}", flush=True)

            with ENGINE_LOCK:
                BOT_STATE["side"] = side
                BOT_STATE["qty"] = qty
                BOT_STATE["fill_price"] = real_fill
                BOT_STATE["entry_id"] = None
                save_state()

                # Condition 14: Stop Loss relative to actual execution price
                initial_stop = round(real_fill - 100.0, 2) if side == "BUY" else round(real_fill + 100.0, 2)
                sl_id = place_stop_loss(target, sl_side, qty, initial_stop, trade_id)
                if sl_id:
                    BOT_STATE["sl_id"] = sl_id
                    BOT_STATE["sl_price"] = initial_stop
                    save_state()
            return

    print(f"[WATCHER TIMEOUT] Order {order_id} unfilled after {timeout_sec}s. Canceling...", flush=True)
    with ENGINE_LOCK:
        cancel_order(order_id)
        if BOT_STATE.get("entry_id") == order_id:
            BOT_STATE["entry_id"] = None
            save_state()

# =============================================================================
# DISPATCH & REVERSAL WORKFLOW
# =============================================================================
def process_entry_signal(action: str, symbol: str, qty: float, price: float, trade_id: int, timeout_sec: int):
    with ENGINE_LOCK:
        target = clean_symbol(symbol)
        side = "BUY" if "BUY" in action else "SELL"
        is_reversal = "REVERSE" in action

        # 1. Condition 7: Immediate Reversal liquidation
        live_qty, live_side = get_exchange_position_state(target)
        if (live_qty > 0.0 and live_side != side) or is_reversal:
            print(f"[REVERSAL TRIGGERED] Immediately closing {live_side} position...", flush=True)
            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])
                BOT_STATE["sl_id"] = None
            if live_qty > 0.0:
                flatten_position(target, "SELL" if live_side == "BUY" else "BUY", live_qty)
            time.sleep(0.2)

        # 2. Cancel open limit orders
        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])
            BOT_STATE["entry_id"] = None

        BOT_STATE["trade_id"] = trade_id
        BOT_STATE["side"] = side
        BOT_STATE["qty"] = qty
        BOT_STATE["entry_id"] = None
        BOT_STATE["sl_id"] = None
        BOT_STATE["fill_price"] = price
        BOT_STATE["sl_price"] = 0.0
        save_state()

        # 3. Post New Limit Order
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
        ok, cid = send_signed_order(payload)
        if ok and cid:
            print(f">>> [LIMIT POSTED] ID: {cid}", flush=True)
            BOT_STATE["entry_id"] = cid
            save_state()

            threading.Thread(
                target=entry_order_watcher,
                args=(target, side, clean_p, trade_id, clean_q, cid, timeout_sec),
                daemon=True
            ).start()

def process_trailing_signal(symbol: str, qty: float, sl_price: float, trade_id: int):
    with ENGINE_LOCK:
        target = clean_symbol(symbol)

        live_qty, live_side = get_exchange_position_state(target)
        if live_qty <= 0.0:
            print(f"[UPDATE_SL DISCARDED] Position is flat on exchange. Purging state.", flush=True)
            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])
            purge_state()
            return

        if BOT_STATE.get("trade_id") != trade_id:
            print(f"[UPDATE_SL IGNORED] Trade ID mismatch ({trade_id} != {BOT_STATE.get('trade_id')}).", flush=True)
            return

        new_stop = float(f"{sl_price:.2f}")
        if new_stop <= 0:
            return

        pos_side = live_side
        sl_side = "SELL" if pos_side == "BUY" else "BUY"
        old_sl_id = BOT_STATE.get("sl_id")
        old_sl_price = BOT_STATE.get("sl_price", 0.0)

        print(f"[UPDATE_SL] Moving {sl_side} Stop from {old_sl_price} to {new_stop}...", flush=True)

        if old_sl_id:
            cancel_order(old_sl_id)
            BOT_STATE["sl_id"] = None
            save_state()

        new_sl_id = place_stop_loss(target, sl_side, live_qty, new_stop, trade_id)
        if new_sl_id:
            BOT_STATE["sl_id"] = new_sl_id
            BOT_STATE["sl_price"] = new_stop
            save_state()
            print(f">>> [STOP UPDATED] Moved to {new_stop} | ID: {new_sl_id}", flush=True)
        else:
            if old_sl_price > 0:
                print(f"[STOP RESTORE] Re-posting previous stop @ {old_sl_price}...", flush=True)
                restored_id = place_stop_loss(target, sl_side, live_qty, old_sl_price, trade_id)
                if restored_id:
                    BOT_STATE["sl_id"] = restored_id
                    save_state()

# =============================================================================
# WEBHOOK RECEIVER
# =============================================================================
@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"status": "bad payload"}

    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Forbidden")

    action = str(data.get("action", "")).upper()
    symbol = str(data.get("symbol", DEFAULT_SYMBOL))
    qty = float(data.get("quantity", 0.05))
    price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))
    timeout_sec = int(data.get("timeout_sec", 300))

    if action == "UPDATE_SL" and sl_price == 0.0 and price > 0.0:
        sl_price = price

    print(f"\n[ALERT RECEIVED] Action: {action} | TradeID: {trade_id} | Qty: {qty} | Price: {price} | SL: {sl_price}", flush=True)

    if action in ["BUY", "SELL", "REVERSE_BUY", "REVERSE_SELL"]:
        threading.Thread(
            target=process_entry_signal,
            args=(action, symbol, qty, price, trade_id, timeout_sec),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=process_trailing_signal,
            args=(symbol, qty, sl_price, trade_id),
            daemon=True
        ).start()

    elif action in ["SL_EXIT", "EXIT", "CLOSE"]:
        with ENGINE_LOCK:
            live_qty, live_side = get_exchange_position_state(symbol)
            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])
            if live_qty > 0.0 and live_side in ["BUY", "SELL"]:
                flatten_position(symbol, "SELL" if live_side == "BUY" else "BUY", live_qty)
            purge_state()

    return {"status": "accepted"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
