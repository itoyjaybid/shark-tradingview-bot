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
# ENVIRONMENT VARIABLES & GLOBAL CONFIGURATION
# =============================================================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

ENTRY_ORDER_EXPIRATION_SECONDS = 600
DEFAULT_SYMBOL = "BTCUSDT"

# Clock synchronization offset (Render container vs Shark server)
SERVER_TIME_OFFSET_MS = 0

# Authoritative Execution State
CURRENT_TRADE_ID = None
CURRENT_POSITION_SIDE = None
CURRENT_POSITION_QTY = 0.0
ACTIVE_SL_CLIENT_IDS = []
ACTIVE_ENTRY_ORDER_ID = None

ORDER_EXECUTION_LOCK = threading.Lock()


# =============================================================================
# SERVER TIME SYNCHRONIZATION & SIGNING
# =============================================================================
def sync_exchange_time():
    """Calculates clock drift between container and Shark Exchange to prevent 4007 Signature Mismatch."""
    global SERVER_TIME_OFFSET_MS
    try:
        resp = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=3)
        if resp.status_code == 200:
            res_data = resp.json()
            server_ts = int(res_data.get("serverTime") or res_data.get("data") or 0)
            if server_ts > 0:
                local_ts = int(time.time() * 1000)
                SERVER_TIME_OFFSET_MS = server_ts - local_ts
                print(f"[TIME SYNC] Synced clock offset with Shark: {SERVER_TIME_OFFSET_MS}ms")
    except Exception as e:
        print(f"[TIME SYNC WARN]: {e}")


def get_synced_timestamp() -> str:
    """Returns local epoch milliseconds adjusted by the exchange server offset."""
    return str(int(time.time() * 1000) + SERVER_TIME_OFFSET_MS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sync_exchange_time()
    yield


app = FastAPI(lifespan=lifespan)


def get_headers(payload_or_querystr: str) -> dict:
    """Computes HMAC-SHA256 signature required by Shark Exchange."""
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
# EXCHANGE DATA LOOKUPS
# =============================================================================
def get_current_market_price(symbol: str = DEFAULT_SYMBOL) -> float:
    target = clean_symbol(symbol)
    ts = get_synced_timestamp()
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
    """Checks exact status of an order via order-detail."""
    target = clean_symbol(symbol)
    ts = get_synced_timestamp()
    variants = [target, f"{target[:-4]}-{target[-4:]}"]

    for sym in variants:
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
    """Checks if order is still active in the limit order book."""
    target = clean_symbol(symbol)
    ts = get_synced_timestamp()
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
    """Cancels order via strict JSON schema (no symbol key, synced timestamp)."""
    ts = get_synced_timestamp()
    payload = {"clientOrderId": str(client_order_id), "timestamp": ts}
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
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid)
    ACTIVE_SL_CLIENT_IDS = []


def cancel_pending_limit_entry():
    global ACTIVE_ENTRY_ORDER_ID
    if ACTIVE_ENTRY_ORDER_ID:
        print(f"[ENTRY CLEANUP] Canceling pending limit entry: {ACTIVE_ENTRY_ORDER_ID}")
        delete_single_order(ACTIVE_ENTRY_ORDER_ID)
        ACTIVE_ENTRY_ORDER_ID = None


def emergency_market_close(symbol: str, side: str, quantity: float):
    """Executes a reduceOnly MARKET order with fresh synced timestamp."""
    sync_exchange_time()
    target = clean_symbol(symbol)
    ts = get_synced_timestamp()
    close_params = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": float(f"{quantity:.4f}"),
        "reduceOnly": True,
        "side": side,
        "symbol": target,
        "timestamp": ts,
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
    """Closes prior position immediately upon reversal alert based on authoritative state."""
    global CURRENT_POSITION_SIDE, CURRENT_POSITION_QTY
    if CURRENT_POSITION_SIDE in ["BUY", "SELL"] and CURRENT_POSITION_QTY > 0:
        close_side = "SELL" if CURRENT_POSITION_SIDE == "BUY" else "BUY"
        print(f"[AUTO-REVERSAL] Active position detected: {CURRENT_POSITION_SIDE} ({CURRENT_POSITION_QTY} BTC). Liquidating via {close_side} MARKET...")
        emergency_market_close(symbol, close_side, CURRENT_POSITION_QTY)
        CURRENT_POSITION_SIDE = None
        CURRENT_POSITION_QTY = 0.0
        time.sleep(0.5)
    else:
        print("[AUTO-REVERSAL] No prior active position tracked in bot state. Proceeding with clean slate.")


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Watchdog thread to sweep market close if price gaps past stop limit."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, CURRENT_POSITION_QTY
    is_buying_back = side == "BUY"
    target = clean_symbol(symbol)

    while sl_client_id in ACTIVE_SL_CLIENT_IDS and CURRENT_TRADE_ID == trade_id:
        time.sleep(0.8)
        curr_price = get_current_market_price(target)
        if curr_price <= 0.0:
            continue

        spike_short = is_buying_back and (curr_price > limit_price)
        spike_long = (not is_buying_back) and (curr_price < limit_price)

        if spike_short or spike_long:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit buffer ({limit_price})!")
            with ORDER_EXECUTION_LOCK:
                if CURRENT_POSITION_SIDE is not None:
                    print(f"[EMERGENCY ACTIVATED] Liquidating immediately via Market Order...")
                    cancel_all_tracked_stops()
                    emergency_market_close(target, side, quantity)
                    CURRENT_POSITION_SIDE = None
                    CURRENT_POSITION_QTY = 0.0
            break


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0) -> str:
    """Submits STOP_LIMIT order with 15 pt limit buffer using fresh synced timestamp."""
    global ACTIVE_SL_CLIENT_IDS, CURRENT_TRADE_ID
    try:
        sync_exchange_time()
        target = clean_symbol(symbol)

        if ref_price <= 0 or (side == "SELL" and stop_price >= ref_price) or (side == "BUY" and stop_price <= ref_price):
            live_mkt = get_current_market_price(target)
            if live_mkt > 0:
                ref_price = live_mkt

        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[GUARD] Long SL {stop_price} >= Market Price {ref_price}. Skipping placement.")
                return None
            if side == "BUY" and stop_price <= ref_price:
                print(f"[GUARD] Short SL {stop_price} <= Market Price {ref_price}. Skipping placement.")
                return None

        offset = 15.0
        limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
        clean_stop = float(f"{stop_price:.2f}")
        clean_limit = float(f"{limit_price:.2f}")
        clean_qty = float(f"{quantity:.4f}")

        ts = get_synced_timestamp()
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
            "timestamp": ts,
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
                    args=(target, side, clean_qty, clean_limit, cid, CURRENT_TRADE_ID),
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
    """
    Watches entry order fill and queries true execution price to place accurate 100 pt stop.
    Captures price improvement without premature fallbacks.
    """
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, CURRENT_POSITION_QTY, ACTIVE_ENTRY_ORDER_ID, ACTIVE_SL_CLIENT_IDS
    target = clean_symbol(symbol)
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling order {order_id} for fill (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")
    time.sleep(1.5)

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
        time.sleep(1.0)

        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting watcher.")
            return

        status, exec_price = check_order_status(order_id, target)
        is_open = is_order_in_open_book(order_id, target)

        if status in ["FILLED", "SUCCESS", "EXECUTED"] or (not is_open and status != "CANCELED"):
            print(f">>> [REAL EXECUTION CONFIRMED] Limit order {order_id} filled!")

            # Micro-poll to guarantee reading the true executed fill price
            real_fill_price = exec_price
            if real_fill_price <= 0:
                for _ in range(4):
                    time.sleep(0.3)
                    _, retry_p = check_order_status(order_id, target)
                    if retry_p > 0:
                        real_fill_price = retry_p
                        break

            # Fallback to limit price if exchange trade records have not settled
            if real_fill_price <= 0:
                real_fill_price = entry_price

            print(f">>> [ACCURATE FILL PRICE RESOLVED]: {real_fill_price}")

            CURRENT_POSITION_SIDE = target_side
            CURRENT_POSITION_QTY = quantity

            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            cancel_all_tracked_stops()
            new_sl_id = place_stop_loss(target, sl_side, quantity, initial_stop, real_fill_price)
            if new_sl_id:
                ACTIVE_SL_CLIENT_IDS.append(new_sl_id)
            return

    print(f"[WATCHER] Limit order timed out after {ENTRY_ORDER_EXPIRATION_SECONDS}s without filling. Canceling...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry()


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    """Liquidates opposing prior position, cleans up stops, syncs clock, and places LIMIT entry."""
    global CURRENT_POSITION_SIDE, CURRENT_POSITION_QTY, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    with ORDER_EXECUTION_LOCK:
        try:
            sync_exchange_time()

            target = clean_symbol(symbol)
            clean_price = float(f"{target_limit_price:.2f}")
            clean_qty = float(f"{quantity:.4f}")

            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            liquidate_prior_position_if_any(target)
            cancel_all_tracked_stops()

            is_buy_intent = "BUY" in action.upper()
            side = "BUY" if is_buy_intent else "SELL"
            CURRENT_TRADE_ID = trade_id

            ts = get_synced_timestamp()
            entry_params = {
                "deviceType": "WEB",
                "marginAsset": "INR",
                "placeType": "ORDER_FORM",
                "price": clean_price,
                "quantity": clean_qty,
                "reduceOnly": False,
                "side": side,
                "symbol": target,
                "timestamp": ts,
                "type": "LIMIT",
                "userCategory": "EXTERNAL"
            }

            entry_body = json.dumps(entry_params, separators=(",", ":"), sort_keys=True)
            entry_headers = get_headers(entry_body)

            print(f"\n[LIMIT ENTRY] Placing {side} {clean_qty} ({target}) @ Limit Price {clean_price} (ts: {ts})...")
            resp_entry = requests.post(
                f"{SHARK_BASE_URL}/v1/order/place-order",
                data=entry_body.encode("utf-8"),
                headers=entry_headers,
                timeout=3
            )

            if resp_entry.status_code in [200, 201]:
                res_data = resp_entry.json()
                ACTIVE_ENTRY_ORDER_ID = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
                print(f">>> [SUCCESS] Limit Entry Posted. Order ID: {ACTIVE_ENTRY_ORDER_ID}")

                threading.Thread(
                    target=wait_for_fill_and_set_sl,
                    args=(target, side, clean_price, trade_id, clean_qty, ACTIVE_ENTRY_ORDER_ID),
                    daemon=True
                ).start()
            else:
                print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

        except Exception as e:
            print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float, trade_id: int, fallback_side: str = None):
    """Updates trailing stop only if the trade is genuinely open in bot state."""
    global CURRENT_POSITION_SIDE, CURRENT_POSITION_QTY, CURRENT_TRADE_ID, ACTIVE_SL_CLIENT_IDS
    try:
        target = clean_symbol(symbol)

        # STRICT GUARD: Only trail if the bot has an active, filled position for THIS exact trade
        if CURRENT_POSITION_SIDE is None or CURRENT_TRADE_ID != trade_id:
            print(f"[REJECTED UPDATE_SL] No confirmed open position for TradeID {trade_id} (Active: {CURRENT_TRADE_ID}, Side: {CURRENT_POSITION_SIDE}). Discarding.")
            return

        resolved_side = CURRENT_POSITION_SIDE

        stop_price = float(f"{sl_price:.2f}")
        ref_price = float(f"{current_price:.2f}") if current_price else 0.0

        if stop_price <= 0:
            return

        with ORDER_EXECUTION_LOCK:
            clean_qty = float(f"{quantity:.4f}")
            sl_side = "SELL" if resolved_side == "BUY" else "BUY"

            print(f"[UPDATE_SL] Moving {sl_side} Stop to {stop_price}...")
            new_sl_id = place_stop_loss(target, sl_side, clean_qty, stop_price, ref_price)

            if new_sl_id:
                old_stops = [cid for cid in ACTIVE_SL_CLIENT_IDS if cid != new_sl_id]
                for old_cid in old_stops:
                    delete_single_order(old_cid)
                ACTIVE_SL_CLIENT_IDS = [new_sl_id]
                print(f">>> [TRAILING SL UPDATED] Active stop is now {new_sl_id} at {stop_price}")

    except Exception as e:
        print(f"[TRAILING SL ERROR]: {e}")


# =============================================================================
# FASTAPI ENDPOINTS
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

    fallback_side = data.get("position_side") or data.get("side")
    if fallback_side:
        fallback_side = str(fallback_side).upper()

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
            args=(symbol, quantity, sl_price, target_limit_price, trade_id, fallback_side),
            daemon=True
        ).start()

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
