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

# Force IPv4 resolution for reliable cloud host routing
def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

# =============================================================================
# ENVIRONMENT VARIABLES & CONFIGURATION
# =============================================================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

ENTRY_ORDER_EXPIRATION_SECONDS = 600
DEFAULT_SYMBOL = "BTCUSDT"
STATE_FILE_PATH = "state.json"

SERVER_TIME_OFFSET_MS = 0
ORDER_EXECUTION_LOCK = threading.Lock()

# Persistent state schema template
BOT_STATE = {
    "trade_id": None,
    "position_side": None,
    "position_qty": 0.0,
    "active_sl_client_ids": [],
    "active_entry_order_id": None
}


# =============================================================================
# STATE PERSISTENCE (Survives Render Container Restarts / Freezes)
# =============================================================================
def load_state_from_disk():
    """Restores bot trade memory across container recycles."""
    global BOT_STATE
    if os.path.exists(STATE_FILE_PATH):
        try:
            with open(STATE_FILE_PATH, "r") as f:
                loaded = json.load(f)
                BOT_STATE.update(loaded)
                print(f"[STATE LOADED] TradeID: {BOT_STATE['trade_id']} | Side: {BOT_STATE['position_side']} | Qty: {BOT_STATE['position_qty']}")
        except Exception as e:
            print(f"[STATE LOAD ERROR]: {e}")


def save_state_to_disk():
    """Flushes active state to persistent file storage immediately."""
    try:
        with open(STATE_FILE_PATH, "w") as f:
            json.dump(BOT_STATE, f, indent=2)
    except Exception as e:
        print(f"[STATE SAVE ERROR]: {e}")


# =============================================================================
# TIME SYNCHRONIZATION & AUTHENTICATION
# =============================================================================
def sync_exchange_time():
    """Calculates clock drift to eliminate 4007 Signature Mismatches."""
    global SERVER_TIME_OFFSET_MS
    try:
        resp = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=3)
        if resp.status_code == 200:
            res_data = resp.json()
            server_ts = int(res_data.get("serverTime") or res_data.get("data") or 0)
            if server_ts > 0:
                local_ts = int(time.time() * 1000)
                diff = server_ts - local_ts
                if abs(diff) < 60000:
                    SERVER_TIME_OFFSET_MS = diff
                    print(f"[TIME SYNC] Offset aligned: {SERVER_TIME_OFFSET_MS}ms")
    except Exception as e:
        print(f"[TIME SYNC WARN]: {e}")


def get_current_ts_int() -> int:
    """Returns local epoch milliseconds adjusted by exchange drift as a raw INT."""
    return int(time.time() * 1000) + SERVER_TIME_OFFSET_MS


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state_from_disk()
    sync_exchange_time()
    yield


app = FastAPI(lifespan=lifespan)


def get_headers(payload_or_querystr: str) -> dict:
    """Generates Shark HMAC-SHA256 authenticated headers."""
    sig = hmac.new(
        SHARK_API_SECRET.encode("utf-8"),
        payload_or_querystr.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return {
        "api-key": SHARK_API_KEY,
        "signature": sig,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }


def clean_symbol(sym: str) -> str:
    if not sym:
        return DEFAULT_SYMBOL
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()


# =============================================================================
# EXCHANGE DATA LOOKUPS & ORDER CHECKS
# =============================================================================
def get_current_market_price(symbol: str = DEFAULT_SYMBOL) -> float:
    target = clean_symbol(symbol)
    ts = str(get_current_ts_int())
    for sym_var in [target, f"{target[:-4]}-{target[-4:]}"]:
        try:
            query = f"symbol={sym_var}&timestamp={ts}"
            headers = get_headers(query)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/ticker/price?{query}", headers=headers, timeout=2)
            if resp.status_code == 200:
                res = resp.json()
                data = res.get("data", res)
                price = float(data.get("price") or data.get("lastPrice") or 0.0)
                if price > 0:
                    return price
        except Exception:
            continue
    return 0.0


def check_order_status(client_order_id: str, symbol: str = DEFAULT_SYMBOL) -> tuple[str, float]:
    """Retrieves authoritative execution status and average executed fill price."""
    target = clean_symbol(symbol)
    ts = str(get_current_ts_int())
    for sym in [target, f"{target[:-4]}-{target[-4:]}"]:
        try:
            query = f"clientOrderId={client_order_id}&symbol={sym}&timestamp={ts}"
            headers = get_headers(query)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{query}", headers=headers, timeout=2)
            if resp.status_code == 200:
                raw = resp.json()
                data = raw.get("data", raw) if isinstance(raw, dict) else raw
                if isinstance(data, dict):
                    status = str(data.get("status") or "").upper()
                    ep = float(data.get("avgPrice") or data.get("executedPrice") or data.get("price") or 0.0)
                    if status:
                        return status, ep
        except Exception:
            continue
    return "UNKNOWN", 0.0


def is_order_in_open_book(client_order_id: str, symbol: str = DEFAULT_SYMBOL) -> bool:
    """Verifies whether an order is actively resting on the order book."""
    target = clean_symbol(symbol)
    ts = str(get_current_ts_int())
    for sym in [target, f"{target[:-4]}-{target[-4:]}"]:
        try:
            query = f"symbol={sym}&timestamp={ts}"
            headers = get_headers(query)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/order/open-orders?{query}", headers=headers, timeout=2)
            if resp.status_code == 200:
                raw = resp.json()
                orders = raw.get("data", raw) if isinstance(raw, dict) else raw
                if isinstance(orders, list):
                    for o in orders:
                        cid = str(o.get("clientOrderId") or o.get("orderId") or "")
                        if client_order_id in cid or cid in client_order_id:
                            return True
        except Exception:
            continue
    return False


def delete_single_order(client_order_id: str) -> bool:
    """Deletes order using pure minimal JSON schema (int timestamp, no symbol)."""
    payload = {
        "clientOrderId": str(client_order_id),
        "timestamp": get_current_ts_int()
    }
    try:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        headers = get_headers(body)
        resp = requests.delete(
            f"{SHARK_BASE_URL}/v1/order/delete-order",
            data=body.encode("utf-8"),
            headers=headers,
            timeout=3
        )
        print(f"[CLEANUP] Cancel ({client_order_id}) -> HTTP {resp.status_code}")
        return resp.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}")
        return False


def cancel_all_tracked_stops():
    """Purges all tracked stop losses on exchange and clears state."""
    sl_ids = list(BOT_STATE.get("active_sl_client_ids", []))
    for cid in sl_ids:
        delete_single_order(cid)
    BOT_STATE["active_sl_client_ids"] = []
    save_state_to_disk()


def cancel_pending_limit_entry():
    """Cancels any active resting limit entry order."""
    entry_id = BOT_STATE.get("active_entry_order_id")
    if entry_id:
        print(f"[ENTRY CLEANUP] Canceling limit entry: {entry_id}")
        delete_single_order(entry_id)
        BOT_STATE["active_entry_order_id"] = None
        save_state_to_disk()


def emergency_market_close(symbol: str, side: str, quantity: float):
    """Executes an immediate reduceOnly MARKET order."""
    target = clean_symbol(symbol)
    close_params = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": float(f"{quantity:.4f}"),
        "reduceOnly": True,
        "side": side,
        "symbol": target,
        "timestamp": get_current_ts_int(),
        "type": "MARKET",
        "userCategory": "EXTERNAL"
    }
    try:
        body = json.dumps(close_params, separators=(",", ":"), sort_keys=True)
        headers = get_headers(body)
        resp = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=body.encode("utf-8"), headers=headers, timeout=3)
        print(f">>> [MARKET FLATTEN RESULT HTTP {resp.status_code}]: {resp.text.strip()}")
    except Exception as e:
        print(f"[EMERGENCY CLOSE ERROR]: {e}")


def liquidate_prior_position_if_any(symbol: str):
    """Guarantees full market liquidation of prior trades before opening new positions."""
    side = BOT_STATE.get("position_side")
    qty = BOT_STATE.get("position_qty", 0.0)

    if side in ["BUY", "SELL"] and qty > 0:
        close_side = "SELL" if side == "BUY" else "BUY"
        print(f"[AUTO-REVERSAL] Active position verified: {side} ({qty} BTC). Liquidating via {close_side} MARKET...")
        emergency_market_close(symbol, close_side, qty)
        BOT_STATE["position_side"] = None
        BOT_STATE["position_qty"] = 0.0
        save_state_to_disk()
        time.sleep(0.5)
    else:
        print("[AUTO-REVERSAL] State clean. No prior active position to liquidate.")


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Sweeps execution via market close if stop price is breached with zero fills."""
    is_buying_back = side == "BUY"
    target = clean_symbol(symbol)

    while sl_client_id in BOT_STATE.get("active_sl_client_ids", []) and BOT_STATE.get("trade_id") == trade_id:
        time.sleep(0.8)
        curr_price = get_current_market_price(target)
        if curr_price <= 0.0:
            continue

        breached = (is_buying_back and curr_price > limit_price) or ((not is_buying_back) and curr_price < limit_price)
        if breached:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit buffer ({limit_price})!")
            with ORDER_EXECUTION_LOCK:
                if BOT_STATE.get("position_side") is not None:
                    print("[EMERGENCY ACTIVATED] Liquidating immediately via Market Order...")
                    cancel_all_tracked_stops()
                    emergency_market_close(target, side, quantity)
                    BOT_STATE["position_side"] = None
                    BOT_STATE["position_qty"] = 0.0
                    save_state_to_disk()
            break


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float) -> str:
    """Submits a STOP_LIMIT order with a 15-point limit buffer directly to Shark."""
    try:
        target = clean_symbol(symbol)
        offset = 15.0
        limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
        clean_stop = float(f"{stop_price:.2f}")
        clean_limit = float(f"{limit_price:.2f}")
        clean_qty = float(f"{quantity:.4f}")

        sl_params = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": clean_limit,
            "quantity": clean_qty,
            "reduceOnly": True,
            "side": side,
            "stopPrice": clean_stop,
            "symbol": target,
            "timestamp": get_current_ts_int(),
            "type": "STOP_LIMIT",
            "userCategory": "EXTERNAL"
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"), sort_keys=True)
        sl_headers = get_headers(sl_body)

        print(f"[SL SUBMIT] Placing {side} STOP_LIMIT @ Stop: {clean_stop}, Limit: {clean_limit}...")
        resp = requests.post(
            f"{SHARK_BASE_URL}/v1/order/place-order",
            data=sl_body.encode("utf-8"),
            headers=sl_headers,
            timeout=3
        )

        if resp.status_code in [200, 201]:
            res = resp.json()
            cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
            if cid:
                print(f">>> [SUCCESS] STOP_LIMIT Placed at {clean_stop} | ID: {cid}")
                threading.Thread(
                    target=monitor_slippage_and_market_close,
                    args=(target, side, clean_qty, clean_limit, cid, BOT_STATE.get("trade_id")),
                    daemon=True
                ).start()
                return cid
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
            return None
    except Exception as e:
        print(f"[SL EXEC ERROR]: {e}")
        return None


def wait_for_fill_and_set_sl(symbol: str, target_side: str, entry_price: float, trade_id: int, quantity: float, order_id: str):
    """Monitors fill and sets protective 100 pt stop based strictly on the accurate fill price."""
    target = clean_symbol(symbol)
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling order {order_id} for execution (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")
    time.sleep(1.5)

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
        time.sleep(1.0)

        if BOT_STATE.get("trade_id") != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Terminating watcher.")
            return

        status, exec_price = check_order_status(order_id, target)
        is_open = is_order_in_open_book(order_id, target)

        if status in ["FILLED", "SUCCESS", "EXECUTED"] or (not is_open and status != "CANCELED"):
            print(f">>> [REAL EXECUTION CONFIRMED] Limit order {order_id} filled!")

            # Micro-poll loop captures true executed price including positive price improvements
            real_fill_price = exec_price
            if real_fill_price <= 0:
                for _ in range(4):
                    time.sleep(0.3)
                    _, retry_p = check_order_status(order_id, target)
                    if retry_p > 0:
                        real_fill_price = retry_p
                        break

            if real_fill_price <= 0:
                real_fill_price = entry_price

            print(f">>> [ACCURATE FILL PRICE RESOLVED]: {real_fill_price}")

            BOT_STATE["position_side"] = target_side
            BOT_STATE["position_qty"] = quantity
            save_state_to_disk()

            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            cancel_all_tracked_stops()
            new_sl_id = place_stop_loss(target, sl_side, quantity, initial_stop)
            if new_sl_id:
                BOT_STATE["active_sl_client_ids"].append(new_sl_id)
                save_state_to_disk()
            return

    print(f"[WATCHER] Limit order timed out after {ENTRY_ORDER_EXPIRATION_SECONDS}s. Canceling...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry()


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    """Executes clean reversal, synchronization, and entry order placement."""
    with ORDER_EXECUTION_LOCK:
        try:
            target = clean_symbol(symbol)
            clean_price = float(f"{target_limit_price:.2f}")
            clean_qty = float(f"{quantity:.4f}")

            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            liquidate_prior_position_if_any(target)
            cancel_all_tracked_stops()

            is_buy_intent = "BUY" in action.upper()
            side = "BUY" if is_buy_intent else "SELL"

            BOT_STATE["trade_id"] = trade_id
            save_state_to_disk()

            ts_int = get_current_ts_int()
            entry_params = {
                "deviceType": "WEB",
                "marginAsset": "INR",
                "placeType": "ORDER_FORM",
                "price": clean_price,
                "quantity": clean_qty,
                "reduceOnly": False,
                "side": side,
                "symbol": target,
                "timestamp": ts_int,
                "type": "LIMIT",
                "userCategory": "EXTERNAL"
            }

            entry_body = json.dumps(entry_params, separators=(",", ":"), sort_keys=True)
            entry_headers = get_headers(entry_body)

            print(f"\n[LIMIT ENTRY] Placing {side} {clean_qty} ({target}) @ Limit Price {clean_price} (ts: {ts_int})...")
            resp_entry = requests.post(
                f"{SHARK_BASE_URL}/v1/order/place-order",
                data=entry_body.encode("utf-8"),
                headers=entry_headers,
                timeout=3
            )

            if resp_entry.status_code in [200, 201]:
                res_data = resp_entry.json()
                cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
                BOT_STATE["active_entry_order_id"] = cid
                save_state_to_disk()
                print(f">>> [SUCCESS] Limit Entry Posted. Order ID: {cid}")

                threading.Thread(
                    target=wait_for_fill_and_set_sl,
                    args=(target, side, clean_price, trade_id, clean_qty, cid),
                    daemon=True
                ).start()
            else:
                print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

        except Exception as e:
            print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, trade_id: int):
    """Places the new tighter stop loss unconditionally and cancels prior stop upon placement."""
    try:
        target = clean_symbol(symbol)
        resolved_side = BOT_STATE.get("position_side")

        if resolved_side is None or BOT_STATE.get("trade_id") != trade_id:
            print(f"[REJECTED UPDATE_SL] No open position for TradeID {trade_id} (Active: {BOT_STATE.get('trade_id')}, Side: {resolved_side}). Discarding.")
            return

        stop_price = float(f"{sl_price:.2f}")
        if stop_price <= 0:
            return

        with ORDER_EXECUTION_LOCK:
            clean_qty = float(f"{quantity:.4f}")
            sl_side = "SELL" if resolved_side == "BUY" else "BUY"

            print(f"[UPDATE_SL] Active position is {resolved_side}. Placing new {sl_side} Stop @ {stop_price}...")
            new_sl_id = place_stop_loss(target, sl_side, clean_qty, stop_price)

            if new_sl_id:
                old_stops = [cid for cid in BOT_STATE.get("active_sl_client_ids", []) if cid != new_sl_id]
                for old_cid in old_stops:
                    delete_single_order(old_cid)
                BOT_STATE["active_sl_client_ids"] = [new_sl_id]
                save_state_to_disk()
                print(f">>> [TRAILING SL UPDATED] Active stop is now {new_sl_id} at {stop_price}")
            else:
                print(f"[UPDATE_SL FAIL] Exchange rejected updated stop at {stop_price}.")

    except Exception as e:
        print(f"[TRAILING SL ERROR]: {e}")


# =============================================================================
# WEBHOOK ENDPOINTS
# =============================================================================
@app.api_route("/", methods=["GET", "HEAD"])
async def home():
    return {"status": "awake", "service": "Shark Trading Bot"}


@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        data = await request.json()
    except Exception:
        return {"status": "ignored_empty"}

    if data.get("secret") != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=403, detail="Invalid secret passphrase")

    action = str(data.get("action", "")).upper()
    symbol = str(data.get("symbol", DEFAULT_SYMBOL))
    quantity = float(data.get("quantity", 0.002))
    target_limit_price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))

    if action == "UPDATE_SL" and sl_price == 0.0 and target_limit_price > 0.0:
        sl_price = target_limit_price

    print(f"\n[ALERT RECEIVED] Action: {action} | Limit/SL Price: {target_limit_price} | sl_price: {sl_price} | TradeID: {trade_id}")

    if action in ["BUY", "SELL"]:
        threading.Thread(
            target=execute_entry_order,
            args=(action, symbol, quantity, target_limit_price, trade_id),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=update_trailing_stop,
            args=(symbol, quantity, sl_price, trade_id),
            daemon=True
        ).start()

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
