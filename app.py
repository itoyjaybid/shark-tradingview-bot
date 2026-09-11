from fastapi import FastAPI, Request, HTTPException
import os
import time
import json
import hmac
import hashlib
import socket
import requests
import threading
import urllib3.util.connection as urllib3_cn

# Force IPv4 resolution on cloud environments
def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

app = FastAPI()

# ==========================================
# CONFIGURATION & RUNTIME STATE
# ==========================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip()
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip()

CURRENT_TRADE_ID = None
CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []
ACTIVE_ENTRY_ORDER_ID = None

ORDER_EXECUTION_LOCK = threading.Lock()


def generate_signature(secret: str, data: str) -> str:
    """Computes HMAC-SHA256 signature strictly on UTF-8 bytes."""
    return hmac.new(secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def get_headers(payload_or_querystr: str) -> dict:
    return {
        "api-key": SHARK_API_KEY,
        "signature": generate_signature(SHARK_API_SECRET, payload_or_querystr),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }


def is_entry_order_open(client_order_id: str, symbol: str) -> bool:
    """Checks whether the limit entry order is still resting on the order book."""
    try:
        ts = str(int(time.time() * 1000))
        query_str = f"symbol={symbol}&timestamp={ts}"
        headers = get_headers(query_str)

        resp = requests.get(f"{SHARK_BASE_URL}/v1/order/open-orders?{query_str}", headers=headers, timeout=5)
        if resp.status_code == 200:
            res_json = resp.json()
            orders = res_json.get("data", []) if isinstance(res_json, dict) else res_json
            for o in orders:
                cid = str(o.get("clientOrderId", "") or o.get("orderId", ""))
                if client_order_id in cid or cid in client_order_id:
                    return True
        return False
    except Exception as e:
        print(f"[STATUS CHECK ERROR]: {e}")
        return False


def get_actual_fill_price(client_order_id: str, symbol: str, fallback_price: float) -> float:
    """
    Directly queries execution receipts via order-detail and trade-history
    to retrieve the exact fill price matched by the exchange engine.
    """
    for attempt in range(4):
        try:
            ts = str(int(time.time() * 1000))
            # 1. Check order details directly
            query_str = f"clientOrderId={client_order_id}&symbol={symbol}&timestamp={ts}"
            headers = get_headers(query_str)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{query_str}", headers=headers, timeout=5)

            if resp.status_code == 200:
                res_data = resp.json()
                data = res_data.get("data", res_data)
                if isinstance(data, dict):
                    # Check for explicit executed/average fill prices
                    fill_p = float(data.get("avgPrice") or data.get("avgFillPrice") or data.get("executedPrice") or 0.0)
                    if fill_p > 0:
                        return fill_p

            # 2. Check trade execution history
            ts2 = str(int(time.time() * 1000))
            query_str2 = f"symbol={symbol}&timestamp={ts2}"
            headers2 = get_headers(query_str2)
            resp2 = requests.get(f"{SHARK_BASE_URL}/v1/order/trade-history?{query_str2}", headers=headers2, timeout=5)

            if resp2.status_code == 200:
                trades_data = resp2.json()
                trades = trades_data.get("data", trades_data) if isinstance(trades_data, dict) else trades_data
                if isinstance(trades, list) and len(trades) > 0:
                    for trade in trades:
                        t_cid = str(trade.get("clientOrderId", "") or trade.get("orderId", ""))
                        if client_order_id in t_cid or t_cid in client_order_id:
                            trade_price = float(trade.get("price") or trade.get("executedPrice") or 0.0)
                            if trade_price > 0:
                                return trade_price
                    # Fallback to the most recent filled trade in the list
                    latest_price = float(trades[0].get("price") or trades[0].get("executedPrice") or 0.0)
                    if latest_price > 0:
                        return latest_price

            # 3. Check active positions endpoint
            ts3 = str(int(time.time() * 1000))
            query_str3 = f"timestamp={ts3}"
            headers3 = get_headers(query_str3)
            resp3 = requests.get(f"{SHARK_BASE_URL}/v1/positions?{query_str3}", headers=headers3, timeout=5)

            if resp3.status_code == 200:
                pos_data = resp3.json()
                positions = pos_data.get("data", pos_data) if isinstance(pos_data, dict) else pos_data
                if isinstance(positions, list):
                    for pos in positions:
                        pos_sym = pos.get("symbol", "").replace(".P", "").replace("-", "").upper()
                        if pos_sym == symbol:
                            pos_entry = float(pos.get("entryPrice") or pos.get("avgPrice") or 0.0)
                            if pos_entry > 0:
                                return pos_entry

        except Exception as e:
            print(f"[FETCH FILL PRICE ATTEMPT {attempt + 1} ERROR]: {e}")

        time.sleep(0.7)

    print(f"[WARN] Unable to extract fill price from API, falling back to: {fallback_price}")
    return fallback_price


def delete_single_order(client_order_id: str) -> bool:
    """Cancels a specific resting order."""
    try:
        ts = str(int(time.time() * 1000))
        payload = {"clientOrderId": str(client_order_id), "timestamp": ts}
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        headers = get_headers(body)

        resp = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=5)
        print(f"[CLEANUP] Canceled order ({client_order_id}) -> HTTP {resp.status_code} | Body: {resp.text.strip()}")
        return resp.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}")
        return False


def cancel_all_tracked_stops():
    """Cancels active stop loss orders."""
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid)
    ACTIVE_SL_CLIENT_IDS = []


def cancel_pending_limit_entry():
    """Cancels unfilled limit entries."""
    global ACTIVE_ENTRY_ORDER_ID
    if ACTIVE_ENTRY_ORDER_ID:
        print(f"[ENTRY CLEANUP] Canceling pending limit entry: {ACTIVE_ENTRY_ORDER_ID}")
        delete_single_order(ACTIVE_ENTRY_ORDER_ID)
        ACTIVE_ENTRY_ORDER_ID = None


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0):
    """Submits a STOP_LIMIT order with numeric float types."""
    global ACTIVE_SL_CLIENT_IDS
    try:
        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[GUARD] Long SL {stop_price} >= Price {ref_price}. Skipping.")
                return
            if side == "BUY" and stop_price <= ref_price:
                print(f"[GUARD] Short SL {stop_price} <= Price {ref_price}. Skipping.")
                return

        time.sleep(0.15)
        offset = 15.0
        limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
        stop_price = round(stop_price, 2)
        clean_qty = round(float(quantity), 4)

        sl_params = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": limit_price,
            "quantity": clean_qty,
            "reduceOnly": True,
            "side": side,
            "stopPrice": stop_price,
            "symbol": symbol,
            "timestamp": str(int(time.time() * 1000)),
            "type": "STOP_LIMIT",
            "userCategory": "EXTERNAL"
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"), sort_keys=True)
        sl_headers = get_headers(sl_body)

        print(f"[SL SUBMIT] Placing {side} STOP_LIMIT @ Stop: {stop_price}, Limit: {limit_price}, Qty: {clean_qty}")
        resp = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=sl_body, headers=sl_headers, timeout=5)

        if resp.status_code in [200, 201]:
            res_data = resp.json()
            cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
            if cid:
                ACTIVE_SL_CLIENT_IDS.append(cid)
                print(f">>> [SUCCESS] STOP_LIMIT Placed at {stop_price} | ID: {cid}")
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
    except Exception as e:
        print(f"[SL EXEC ERROR]: {e}")


def wait_for_fill_and_set_sl(clean_symbol: str, target_side: str, entry_price: float, trade_id: int, quantity: float):
    """
    Monitors entry order execution. Upon fill, queries the exact fill execution
    receipt to calculate and submit a true 100-point stop loss.
    """
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, ACTIVE_ENTRY_ORDER_ID
    is_buy = target_side == "BUY"

    print(f"[WATCHER] Polling Shark Exchange for {target_side} fill...")

    # Wait up to 4 minutes (120 cycles * 2 seconds)
    for _ in range(300):
        time.sleep(2.0)
        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting.")
            return

        if not ACTIVE_ENTRY_ORDER_ID:
            return

        still_open = is_entry_order_open(ACTIVE_ENTRY_ORDER_ID, clean_symbol)

        if not still_open:
            print(f">>> [LIMIT FILLED] Fetching true executed fill price...")
            CURRENT_POSITION_SIDE = target_side

            # Extract true filled execution price across exchange receipts
            real_fill_price = get_actual_fill_price(ACTIVE_ENTRY_ORDER_ID, clean_symbol, entry_price)
            print(f">>> [EXECUTION CONFIRMED] Real Entry Price: {real_fill_price}")

            # Calculate exact 100-point offset from real entry
            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            place_stop_loss(clean_symbol, sl_side, quantity, initial_stop, real_fill_price)
            return

    print(f"[WATCHER] Limit order timed out without fill. Canceling...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry()
        CURRENT_POSITION_SIDE = None


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
            target_qty = round(float(quantity), 4)
            clean_price = round(float(target_limit_price), 2)

            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            is_buy_intent = "BUY" in action.upper()
            side = "BUY" if is_buy_intent else "SELL"
            CURRENT_TRADE_ID = trade_id

            entry_params = {
                "deviceType": "WEB",
                "marginAsset": "INR",
                "placeType": "ORDER_FORM",
                "price": clean_price,
                "quantity": target_qty,
                "reduceOnly": False,
                "side": side,
                "symbol": clean_symbol,
                "timestamp": str(int(time.time() * 1000)),
                "type": "LIMIT",
                "userCategory": "EXTERNAL"
            }

            entry_body = json.dumps(entry_params, separators=(",", ":"), sort_keys=True)
            entry_headers = get_headers(entry_body)

            print(f"\n[LIMIT ENTRY] Placing {side} {target_qty} {clean_symbol} @ Limit Price {clean_price}...")
            resp_entry = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=entry_body, headers=entry_headers, timeout=5)

            if resp_entry.status_code in [200, 201]:
                res_data = resp_entry.json()
                ACTIVE_ENTRY_ORDER_ID = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
                print(f">>> [SUCCESS] Limit Entry Posted. Order ID: {ACTIVE_ENTRY_ORDER_ID}")

                threading.Thread(
                    target=wait_for_fill_and_set_sl,
                    args=(clean_symbol, side, clean_price, trade_id, target_qty),
                    daemon=True
                ).start()
            else:
                print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

        except Exception as e:
            print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float, trade_id: int):
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

            if CURRENT_TRADE_ID is not None and trade_id != CURRENT_TRADE_ID:
                print(f"[DESYNC GUARD] Alert trade_id ({trade_id}) != current ({CURRENT_TRADE_ID}). Ignored.")
                return

            if CURRENT_POSITION_SIDE is None:
                print(f"[REJECTED UPDATE_SL] No open trade active on exchange. Discarding trail.")
                return

            clean_qty = round(float(quantity), 4)
            stop_price = round(float(sl_price), 2)
            ref_price = float(current_price) if current_price else 0.0

            if stop_price > 0:
                sl_side = "SELL" if CURRENT_POSITION_SIDE == "BUY" else "BUY"
                cancel_all_tracked_stops()
                place_stop_loss(clean_symbol, sl_side, clean_qty, stop_price, ref_price)

        except Exception as e:
            print(f"[TRAILING SL ERROR]: {e}")


# ==========================================
# FASTAPI ENDPOINTS
# ==========================================
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
    symbol = str(data.get("symbol", "BTCUSDT"))
    quantity = float(data.get("quantity", 0.002))
    target_limit_price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))

    print(f"\n[ALERT RECEIVED] Action: {action} | Limit Price: {target_limit_price} | TradeID: {trade_id}")

    if action in ["BUY", "SELL"]:
        threading.Thread(
            target=execute_entry_order,
            args=(action, symbol, quantity, target_limit_price, trade_id),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=update_trailing_stop,
            args=(symbol, quantity, sl_price, target_limit_price, trade_id),
            daemon=True
        ).start()

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
