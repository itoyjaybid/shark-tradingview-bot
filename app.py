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

# Condition 6 & 10: Limit entry timeout duration (seconds)
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


def get_symbol_variants(symbol: str) -> list[str]:
    """Generates BTC-USDT and BTCUSDT representations."""
    clean = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
    hyphen = f"{clean[:-4]}-{clean[-4:]}" if clean.endswith("USDT") else clean
    return [hyphen, clean]


def is_entry_order_open(client_order_id: str, symbol: str) -> bool:
    """Verifies whether an entry limit order is active on the order book."""
    try:
        ts = str(int(time.time() * 1000))
        for sym in get_symbol_variants(symbol):
            query_str = f"symbol={sym}&timestamp={ts}"
            headers = get_headers(query_str)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/order/open-orders?{query_str}", headers=headers, timeout=2)
            if resp.status_code == 200:
                res = resp.json()
                orders = res.get("data", []) if isinstance(res, dict) else res
                if isinstance(orders, list):
                    for o in orders:
                        cid = str(o.get("clientOrderId", "") or o.get("orderId", ""))
                        if client_order_id in cid or cid in client_order_id:
                            return True
        return False
    except Exception as e:
        print(f"[STATUS CHECK ERROR]: {e}")
        return True  # Fail-safe: assume still open during transient network drops


def get_current_market_price(symbol: str) -> float:
    """Fetches real-time price from the exchange ticker."""
    try:
        ts = str(int(time.time() * 1000))
        for sym in get_symbol_variants(symbol):
            query_str = f"symbol={sym}&timestamp={ts}"
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


def extract_position_from_item(pos: dict, clean_target: str):
    """Parses position dictionary across various exchange schemas."""
    raw_sym = str(pos.get("symbol") or pos.get("market") or pos.get("contract") or "").replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
    if raw_sym == clean_target:
        for key in ["positionAmt", "size", "currentQty", "qty", "openSize", "contracts", "holdAmount", "volume", "contractVal"]:
            val = pos.get(key)
            if val is not None:
                try:
                    amt = float(val)
                    if amt != 0:
                        side_val = str(pos.get("side") or pos.get("positionSide") or ("BUY" if amt > 0 else "SELL")).upper()
                        pos_side = "BUY" if ("BUY" in side_val or "LONG" in side_val) else "SELL"
                        entry_p = float(pos.get("entryPrice") or pos.get("avgPrice") or pos.get("avgCost") or 0.0)
                        return abs(amt), pos_side, entry_p
                except (ValueError, TypeError):
                    continue
    return 0.0, None, 0.0


def get_active_position_details(symbol: str):
    """
    Condition 13: Queries Shark Exchange positions using recursive payload unwrapping.
    Returns (position_size, side, entry_price).
    """
    try:
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        ts = str(int(time.time() * 1000))

        variants = get_symbol_variants(symbol)
        queries = [f"timestamp={ts}"] + [f"symbol={v}&timestamp={ts}" for v in variants]

        for q in queries:
            headers = get_headers(q)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/positions?{q}", headers=headers, timeout=2)

            if resp.status_code == 200:
                raw_json = resp.json()
                items = []
                if isinstance(raw_json, list):
                    items = raw_json
                elif isinstance(raw_json, dict):
                    data_block = raw_json.get("data") or raw_json.get("result") or raw_json.get("positions") or raw_json
                    if isinstance(data_block, list):
                        items = data_block
                    elif isinstance(data_block, dict):
                        for sub in ["positions", "list", "rows", "data"]:
                            if isinstance(data_block.get(sub), list):
                                items = data_block.get(sub)
                                break
                        if not items:
                            items = [data_block]

                for pos in items:
                    if isinstance(pos, dict):
                        amt, side, ep = extract_position_from_item(pos, clean_target)
                        if amt > 0:
                            return amt, side, ep
    except Exception as e:
        print(f"[POSITION CHECK ERROR]: {e}")

    return 0.0, None, 0.0


def check_order_strictly_filled(client_order_id: str, symbol: str) -> tuple[bool, float]:
    """
    Condition 14: Accurately checks whether the limit entry was executed on the exchange.
    Returns (is_filled, executed_avg_price).
    """
    ts = str(int(time.time() * 1000))
    variants = get_symbol_variants(symbol)

    for sym in variants:
        try:
            # 1. Order details check
            q_detail = f"clientOrderId={client_order_id}&symbol={sym}&timestamp={ts}"
            headers_detail = get_headers(q_detail)
            resp_detail = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{q_detail}", headers=headers_detail, timeout=2)
            if resp_detail.status_code == 200:
                data = resp_detail.json()
                item = data.get("data", data) if isinstance(data, dict) else data
                if isinstance(item, dict):
                    status = str(item.get("status", "")).upper()
                    if status in ["FILLED", "SUCCESS", "EXECUTED"]:
                        exec_price = float(item.get("avgPrice") or item.get("price") or item.get("executedPrice") or 0.0)
                        return True, exec_price

            # 2. Order history check
            q_hist = f"clientOrderId={client_order_id}&symbol={sym}&timestamp={ts}"
            headers_hist = get_headers(q_hist)
            resp_hist = requests.get(f"{SHARK_BASE_URL}/v1/order/order-history?{q_hist}", headers=headers_hist, timeout=2)
            if resp_hist.status_code == 200:
                hist_data = resp_hist.json()
                orders = hist_data.get("data", hist_data) if isinstance(hist_data, dict) else hist_data
                if isinstance(orders, list):
                    for o in orders:
                        cid = str(o.get("clientOrderId", "") or o.get("orderId", ""))
                        if client_order_id in cid or cid in client_order_id:
                            status = str(o.get("status", "")).upper()
                            if status in ["FILLED", "SUCCESS", "EXECUTED"]:
                                avg_p = float(o.get("avgPrice") or o.get("price") or 0.0)
                                return True, avg_p
        except Exception:
            pass

    return False, 0.0


def delete_single_order(client_order_id: str) -> bool:
    """
    Conditions 6 & 7: Cancels order via exact schema matching Shark Exchange's specification.
    Omits 'symbol' completely to eliminate HTTP 400 schema rejections and signature mismatches.
    """
    ts = str(int(time.time() * 1000))
    payload_variants = [
        {"clientOrderId": str(client_order_id), "timestamp": ts},
        {"orderId": str(client_order_id), "timestamp": ts}
    ]

    for payload in payload_variants:
        try:
            body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            headers_b = get_headers(body)
            resp_b = requests.delete(
                f"{SHARK_BASE_URL}/v1/order/delete-order",
                data=body.encode("utf-8"),
                headers=headers_b,
                timeout=3
            )
            print(f"[CLEANUP] Cancel order ({client_order_id}) -> HTTP {resp_b.status_code} | {resp_b.text.strip()}")
            if resp_b.status_code in [200, 201, 204]:
                return True
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
    """Executes a market order with reduceOnly=True to immediately flatten a position."""
    try:
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        ts = str(int(time.time() * 1000))

        close_params = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "quantity": float(f"{quantity:.4f}"),
            "reduceOnly": True,
            "side": side,
            "symbol": clean_target,
            "timestamp": ts,
            "type": "MARKET",
            "userCategory": "EXTERNAL"
        }
        body = json.dumps(close_params, separators=(",", ":"), sort_keys=True)
        headers = get_headers(body)

        resp = requests.post(
            f"{SHARK_BASE_URL}/v1/order/place-order",
            data=body.encode("utf-8"),
            headers=headers,
            timeout=3
        )
        print(f">>> [MARKET FLATTEN RESULT HTTP {resp.status_code}]: {resp.text.strip()}")
    except Exception as e:
        print(f"[EMERGENCY CLOSE ERROR]: {e}")


def close_position_immediately(symbol: str):
    """Condition 7 & 13: Flattens open position before opening an opposing position."""
    size, side, _ = get_active_position_details(symbol)
    if size > 0 and side:
        close_side = "SELL" if side == "BUY" else "BUY"
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        print(f"[AUTO-REVERSAL] Liquidating prior {side} ({size} BTC) via {close_side} MARKET order...")
        emergency_market_close(clean_target, close_side, size)
        time.sleep(0.5)


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Condition 3: Watchdog thread to market close position if price blows through the stop limit."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE
    is_buying_back = side == "BUY"
    clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()

    while sl_client_id in ACTIVE_SL_CLIENT_IDS and CURRENT_TRADE_ID == trade_id:
        time.sleep(0.8)
        curr_price = get_current_market_price(clean_target)
        if curr_price <= 0.0:
            continue

        spike_short_breached = is_buying_back and (curr_price > limit_price)
        spike_long_breached = (not is_buying_back) and (curr_price < limit_price)

        if spike_short_breached or spike_long_breached:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit ceiling ({limit_price})!")
            with ORDER_EXECUTION_LOCK:
                size, _, _ = get_active_position_details(clean_target)
                if size > 0:
                    print(f"[EMERGENCY ACTIVATED] Liquidating immediately via Market Order...")
                    cancel_all_tracked_stops()
                    emergency_market_close(clean_target, side, quantity)
                    CURRENT_POSITION_SIDE = None
                else:
                    print(f"[MONITOR] Position already closed. Watchdog standing down.")
            break


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0) -> str:
    """Condition 3: Submits a STOP_LIMIT order with a 15 pt limit buffer."""
    global ACTIVE_SL_CLIENT_IDS, CURRENT_TRADE_ID
    try:
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()

        if ref_price <= 0 or (side == "SELL" and stop_price >= ref_price) or (side == "BUY" and stop_price <= ref_price):
            live_mkt = get_current_market_price(clean_target)
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
        clean_stop_price = float(f"{stop_price:.2f}")
        clean_limit_price = float(f"{limit_price:.2f}")
        clean_qty = float(f"{quantity:.4f}")

        ts = str(int(time.time() * 1000))

        sl_params = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": clean_limit_price,
            "quantity": clean_qty,
            "reduceOnly": True,
            "side": side,
            "stopPrice": clean_stop_price,
            "symbol": clean_target,
            "timestamp": ts,
            "type": "STOP_LIMIT",
            "userCategory": "EXTERNAL"
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"), sort_keys=True)
        sl_headers = get_headers(sl_body)

        print(f"[SL SUBMIT] Placing {side} STOP_LIMIT @ Stop: {clean_stop_price}, Limit: {clean_limit_price}, Qty: {clean_qty}")
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
                print(f">>> [SUCCESS] STOP_LIMIT Placed at {clean_stop_price} | ID: {cid}")
                threading.Thread(
                    target=monitor_slippage_and_market_close,
                    args=(clean_target, side, clean_qty, clean_limit_price, cid, CURRENT_TRADE_ID),
                    daemon=True
                ).start()
                return cid
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
            return None
    except Exception as e:
        print(f"[SL EXEC ERROR]: {e}")
        return None


def wait_for_fill_and_set_sl(clean_symbol: str, target_side: str, entry_price: float, trade_id: int, quantity: float):
    """
    Conditions 6, 13 & 14:
    Strictly verifies executed fill on the exchange before placing Stop Loss.
    Never fires premature stops while the limit order is still resting unfilled.
    """
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, ACTIVE_ENTRY_ORDER_ID, ACTIVE_SL_CLIENT_IDS
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling Shark Exchange for {target_side} fill (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")

    # Grace period: allow 2.0s for the newly placed limit order to register on exchange books
    time.sleep(2.0)

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
        time.sleep(1.0)

        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting watcher.")
            return

        if not ACTIVE_ENTRY_ORDER_ID:
            return

        # 1. Primary check: Has an active position actually opened on the exchange?
        pos_size, live_side, live_ep = get_active_position_details(clean_symbol)

        # 2. Secondary check: Does order details explicitly confirm FILLED status?
        is_filled, exec_price = check_order_strictly_filled(ACTIVE_ENTRY_ORDER_ID, clean_symbol)

        # Proceed only if Shark Exchange verifies real execution
        if (pos_size > 0 and live_side == target_side) or is_filled:
            print(f">>> [REAL EXECUTION CONFIRMED] Limit Order filled on Shark Exchange!")
            CURRENT_POSITION_SIDE = target_side

            real_fill_price = live_ep if live_ep > 0 else (exec_price if exec_price > 0 else entry_price)
            print(f">>> [TRUE FILL PRICE FROM EXCHANGE]: {real_fill_price}")

            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            cancel_all_tracked_stops()

            new_sl_id = place_stop_loss(clean_symbol, sl_side, quantity, initial_stop, real_fill_price)
            if new_sl_id:
                ACTIVE_SL_CLIENT_IDS.append(new_sl_id)
            return

    # 600s Expiration Reached
    print(f"[WATCHER] Limit order timed out after {ENTRY_ORDER_EXPIRATION_SECONDS}s without filling. Canceling order...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry()
        CURRENT_POSITION_SIDE = None


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    """Conditions 1, 2, 7, 8, 13: Handles reversals, liquidations, and limit entries."""
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
            clean_price = float(f"{target_limit_price:.2f}")
            clean_qty = float(f"{quantity:.4f}")

            # 1. Clean prior pending entry and stop-loss orders
            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            # 2. Condition 7 & 13: Close previous position before reversing
            close_position_immediately(clean_symbol)
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
                "symbol": clean_symbol,
                "timestamp": ts,
                "type": "LIMIT",
                "userCategory": "EXTERNAL"
            }

            entry_body = json.dumps(entry_params, separators=(",", ":"), sort_keys=True)
            entry_headers = get_headers(entry_body)

            print(f"\n[LIMIT ENTRY] Placing {side} {clean_qty} {clean_symbol} @ Limit Price {clean_price}...")
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
                    args=(clean_symbol, side, clean_price, trade_id, clean_qty),
                    daemon=True
                ).start()
            else:
                print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

        except Exception as e:
            print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float, trade_id: int, fallback_side: str = None):
    """
    Evaluates position status instantly (no lock block) and updates the trailing stop cleanly.
    """
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_SL_CLIENT_IDS
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

        # Immediate check outside of lock to avoid stalling when flat
        pos_size, live_side, _ = get_active_position_details(clean_symbol)

        resolved_side = None
        if pos_size > 0 and live_side is not None:
            resolved_side = live_side
            CURRENT_POSITION_SIDE = live_side
        elif CURRENT_POSITION_SIDE is not None:
            resolved_side = CURRENT_POSITION_SIDE
        elif fallback_side in ["BUY", "SELL"] and CURRENT_TRADE_ID == trade_id:
            resolved_side = fallback_side
            CURRENT_POSITION_SIDE = fallback_side

        # Immediate rejection if no position exists anywhere
        if resolved_side is None:
            print(f"[REJECTED UPDATE_SL] No open trade found on exchange or in bot state. Discarding trailing SL.")
            return

        stop_price = float(f"{sl_price:.2f}")
        ref_price = float(f"{current_price:.2f}") if current_price else 0.0

        if stop_price <= 0:
            print(f"[UPDATE_SL] Invalid stop price {stop_price}. Skipped.")
            return

        # Acquire lock only during order modification
        with ORDER_EXECUTION_LOCK:
            CURRENT_TRADE_ID = trade_id
            clean_qty = float(f"{quantity:.4f}")
            sl_side = "SELL" if resolved_side == "BUY" else "BUY"

            print(f"[UPDATE_SL] Confirmed position side {resolved_side}. Placing {sl_side} Stop @ {stop_price}...")
            new_sl_id = place_stop_loss(clean_symbol, sl_side, clean_qty, stop_price, ref_price)

            if new_sl_id:
                old_stops = [cid for cid in ACTIVE_SL_CLIENT_IDS if cid != new_sl_id]
                for old_cid in old_stops:
                    delete_single_order(old_cid)
                ACTIVE_SL_CLIENT_IDS = [new_sl_id]
                print(f">>> [TRAILING SL UPDATED] Active stop is now {new_sl_id} at {stop_price}")
            else:
                print(f"[WARN] New SL placement failed or skipped. Keeping existing stops intact.")

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
