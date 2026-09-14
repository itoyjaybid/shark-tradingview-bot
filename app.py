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

# Force IPv4 network routing for cloud deployment reliability
def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

app = FastAPI()

# =============================================================================
# CONFIGURATION & RUNTIME STATE
# =============================================================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

ENTRY_ORDER_EXPIRATION_SECONDS = 600

CURRENT_TRADE_ID = None
CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []
ACTIVE_ENTRY_ORDER_ID = None

ORDER_EXECUTION_LOCK = threading.Lock()


def get_headers(payload_or_querystr: str) -> dict:
    """Generates HMAC-SHA256 signature for Shark Exchange authentication."""
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


def clean_sym(sym: str) -> str:
    """Always standardizes to plain BTCUSDT format accepted by Shark Exchange."""
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()


def get_current_market_price(symbol: str) -> float:
    """Fetches real-time price from the exchange ticker."""
    try:
        ts = str(int(time.time() * 1000))
        target = clean_sym(symbol)
        for sym_var in [target, f"{target[:-4]}-{target[-4:]}"]:
            query_str = f"symbol={sym_var}&timestamp={ts}"
            headers = get_headers(query_str)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/ticker/price?{query_str}", headers=headers, timeout=2)
            if resp.status_code == 200:
                res = resp.json()
                data = res.get("data", res)
                price = float(data.get("price") or data.get("lastPrice") or 0.0)
                if price > 0:
                    return price
    except Exception as e:
        print(f"[TICKER FETCH ERROR]: {e}")
    return 0.0


def get_active_position_details(symbol: str):
    """
    Directly inspects open positions from Shark Exchange.
    Returns (position_size, side, entry_price).
    """
    try:
        target = clean_sym(symbol)
        ts = str(int(time.time() * 1000))

        # Query all positions directly
        q = f"timestamp={ts}"
        headers = get_headers(q)
        resp = requests.get(f"{SHARK_BASE_URL}/v1/positions?{q}", headers=headers, timeout=3)

        if resp.status_code == 200:
            raw = resp.json()
            data_list = raw.get("data", raw) if isinstance(raw, dict) else raw
            if isinstance(data_list, dict):
                data_list = data_list.get("positions", [data_list])

            if isinstance(data_list, list):
                for p in data_list:
                    if not isinstance(p, dict):
                        continue
                    p_sym = clean_sym(str(p.get("symbol") or p.get("market") or ""))
                    if p_sym == target or target in p_sym:
                        # Extract quantity
                        for k in ["positionAmt", "size", "currentQty", "qty", "openSize", "contracts"]:
                            if p.get(k) is not None:
                                try:
                                    amt = abs(float(p[k]))
                                    if amt > 0:
                                        side_raw = str(p.get("side") or p.get("positionSide") or ("BUY" if float(p[k]) > 0 else "SELL")).upper()
                                        pos_side = "BUY" if ("BUY" in side_raw or "LONG" in side_raw) else "SELL"
                                        ep = float(p.get("entryPrice") or p.get("avgPrice") or 0.0)
                                        return amt, pos_side, ep
                                except (ValueError, TypeError):
                                    continue
    except Exception as e:
        print(f"[POSITION CHECK ERROR]: {e}")
    return 0.0, None, 0.0


def delete_single_order(client_order_id: str) -> bool:
    """Cancels order using the exact body schema accepted by Shark Exchange."""
    ts = str(int(time.time() * 1000))
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
        print(f"[CLEANUP] Cancel order ({client_order_id}) -> HTTP {resp.status_code}")
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
    """Executes market order to immediately flatten an existing position."""
    target = clean_sym(symbol)
    ts = str(int(time.time() * 1000))
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


def close_position_immediately(symbol: str):
    """Flattens position before executing a reverse signal."""
    size, side, _ = get_active_position_details(symbol)
    if size > 0 and side:
        close_side = "SELL" if side == "BUY" else "BUY"
        print(f"[AUTO-REVERSAL] Liquidating prior {side} ({size} BTC) via {close_side} MARKET order...")
        emergency_market_close(symbol, close_side, size)
        time.sleep(0.5)


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Condition 3: Watchdog thread to market close position if price blows through the stop limit."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE
    is_buying_back = side == "BUY"

    while sl_client_id in ACTIVE_SL_CLIENT_IDS and CURRENT_TRADE_ID == trade_id:
        time.sleep(0.8)
        curr_price = get_current_market_price(symbol)
        if curr_price <= 0.0:
            continue

        spike_short_breached = is_buying_back and (curr_price > limit_price)
        spike_long_breached = (not is_buying_back) and (curr_price < limit_price)

        if spike_short_breached or spike_long_breached:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit ceiling ({limit_price})!")
            with ORDER_EXECUTION_LOCK:
                size, _, _ = get_active_position_details(symbol)
                if size > 0:
                    print(f"[EMERGENCY ACTIVATED] Liquidating immediately via Market Order...")
                    cancel_all_tracked_stops()
                    emergency_market_close(symbol, side, quantity)
                    CURRENT_POSITION_SIDE = None
                else:
                    print(f"[MONITOR] Position already closed. Watchdog standing down.")
            break


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0) -> str:
    """Submits STOP_LIMIT order with 15 pt buffer using clean BTCUSDT symbol."""
    global ACTIVE_SL_CLIENT_IDS, CURRENT_TRADE_ID
    try:
        target = clean_sym(symbol)
        offset = 15.0
        limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)
        clean_stop = float(f"{stop_price:.2f}")
        clean_limit = float(f"{limit_price:.2f}")
        clean_qty = float(f"{quantity:.4f}")

        ts = str(int(time.time() * 1000))
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


def wait_for_fill_and_set_sl(symbol: str, target_side: str, entry_price: float, trade_id: int, quantity: float):
    """
    Watches exchange for real position fill and places STOP_LIMIT.
    """
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, ACTIVE_ENTRY_ORDER_ID, ACTIVE_SL_CLIENT_IDS
    target = clean_sym(symbol)
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling Shark Exchange for {target_side} fill (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")

    # Grace period: allow 1.5s for initial placement
    time.sleep(1.5)

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
        time.sleep(0.8)

        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting watcher.")
            return

        # Check directly if active position exists on the exchange
        pos_size, live_side, live_ep = get_active_position_details(target)

        if pos_size > 0 and live_side == target_side:
            print(f">>> [REAL EXECUTION CONFIRMED] Position detected on Shark Exchange! Size: {pos_size}")
            CURRENT_POSITION_SIDE = target_side

            # Use exchange entry price if indexed, else limit entry price
            real_fill_price = live_ep if live_ep > 0 else entry_price
            print(f">>> [TRUE FILL PRICE]: {real_fill_price}")

            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            cancel_all_tracked_stops()

            new_sl_id = place_stop_loss(target, sl_side, quantity, initial_stop, real_fill_price)
            if new_sl_id:
                ACTIVE_SL_CLIENT_IDS.append(new_sl_id)
            return

    print(f"[WATCHER] Limit order timed out after {ENTRY_ORDER_EXPIRATION_SECONDS}s. Canceling order...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry()
        CURRENT_POSITION_SIDE = None


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    """Places LIMIT entry order directly using verified BTCUSDT ticker."""
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    with ORDER_EXECUTION_LOCK:
        try:
            target = clean_sym(symbol)
            clean_price = float(f"{target_limit_price:.2f}")
            clean_qty = float(f"{quantity:.4f}")

            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            close_position_immediately(target)
            cancel_all_tracked_stops()

            is_buy_intent = "BUY" in action.upper()
            side = "BUY" if is_buy_intent else "SELL"
            CURRENT_TRADE_ID = trade_id

            ts = str(int(time.time() * 1000))
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

            print(f"\n[LIMIT ENTRY] Placing {side} {clean_qty} ({target}) @ Limit Price {clean_price}...")
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
                    args=(target, side, clean_price, trade_id, clean_qty),
                    daemon=True
                ).start()
            else:
                print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

        except Exception as e:
            print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float, trade_id: int, fallback_side: str = None):
    """Evaluates position state instantly and updates trailing stop without stalling."""
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_SL_CLIENT_IDS
    try:
        target = clean_sym(symbol)
        pos_size, live_side, _ = get_active_position_details(target)

        resolved_side = None
        if pos_size > 0 and live_side is not None:
            resolved_side = live_side
            CURRENT_POSITION_SIDE = live_side
        elif CURRENT_POSITION_SIDE is not None:
            resolved_side = CURRENT_POSITION_SIDE
        elif fallback_side in ["BUY", "SELL"] and CURRENT_TRADE_ID == trade_id:
            resolved_side = fallback_side
            CURRENT_POSITION_SIDE = fallback_side

        if resolved_side is None:
            print(f"[REJECTED UPDATE_SL] No open trade found on exchange. Discarding trailing SL.")
            return

        stop_price = float(f"{sl_price:.2f}")
        ref_price = float(f"{current_price:.2f}") if current_price else 0.0

        if stop_price <= 0:
            return

        with ORDER_EXECUTION_LOCK:
            CURRENT_TRADE_ID = trade_id
            clean_qty = float(f"{quantity:.4f}")
            sl_side = "SELL" if resolved_side == "BUY" else "BUY"

            print(f"[UPDATE_SL] Confirmed position side {resolved_side}. Placing {sl_side} Stop @ {stop_price}...")
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
    symbol = str(data.get("symbol", "BTCUSDT"))
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
