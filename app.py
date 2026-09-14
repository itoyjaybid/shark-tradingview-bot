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
    """Generates both hyphenated and non-hyphenated formats (e.g., BTC-USDT and BTCUSDT)."""
    clean_no_hyphen = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
    hyphen_variant = f"{clean_no_hyphen[:-4]}-{clean_no_hyphen[-4:]}" if clean_no_hyphen.endswith("USDT") else clean_no_hyphen
    return [hyphen_variant, clean_no_hyphen]


def is_entry_order_open(client_order_id: str, symbol: str) -> bool:
    """Verifies whether an entry limit order is still resting on the live order book."""
    try:
        ts = str(int(time.time() * 1000))
        for sym_var in get_symbol_variants(symbol):
            query_str = f"symbol={sym_var}&timestamp={ts}"
            headers = get_headers(query_str)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/order/open-orders?{query_str}", headers=headers, timeout=5)
            if resp.status_code == 200:
                res_json = resp.json()
                orders = res_json.get("data", []) if isinstance(res_json, dict) else res_json
                if isinstance(orders, list):
                    for o in orders:
                        cid = str(o.get("clientOrderId", "") or o.get("orderId", ""))
                        if client_order_id in cid or cid in client_order_id:
                            return True
        return False
    except Exception as e:
        print(f"[STATUS CHECK ERROR]: {e}")
        return False


def get_current_market_price(symbol: str) -> float:
    """Retrieves current market price from Shark Exchange ticker endpoint."""
    try:
        ts = str(int(time.time() * 1000))
        for sym_var in get_symbol_variants(symbol):
            query_str = f"symbol={sym_var}&timestamp={ts}"
            headers = get_headers(query_str)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/ticker/price?{query_str}", headers=headers, timeout=3)
            if resp.status_code == 200:
                res_data = resp.json()
                data = res_data.get("data", res_data)
                price = float(data.get("price") or data.get("lastPrice") or 0.0)
                if price > 0:
                    return price
    except Exception as e:
        print(f"[TICKER FETCH ERROR]: {e}")
    return 0.0


def extract_position_from_item(pos: dict, clean_target: str):
    """Helper to inspect a position dictionary across various exchange schemas."""
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
                        return abs(amt), pos_side
                except (ValueError, TypeError):
                    continue
    return 0.0, None


def get_active_position_details(symbol: str):
    """
    Condition 13: Queries Shark Exchange positions using recursive response unwrapping.
    Returns (position_size, side).
    """
    try:
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        ts = str(int(time.time() * 1000))

        variants = get_symbol_variants(symbol)
        queries = [f"timestamp={ts}"] + [f"symbol={v}&timestamp={ts}" for v in variants]

        for q in queries:
            headers = get_headers(q)
            resp = requests.get(f"{SHARK_BASE_URL}/v1/positions?{q}", headers=headers, timeout=4)

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
                        for sub_key in ["positions", "list", "rows", "data"]:
                            if isinstance(data_block.get(sub_key), list):
                                items = data_block.get(sub_key)
                                break
                        if not items:
                            items = [data_block]

                for pos in items:
                    if isinstance(pos, dict):
                        amt, side = extract_position_from_item(pos, clean_target)
                        if amt > 0:
                            print(f">>> [EXCHANGE POSITION DETECTED] Found {side} position: {amt} {clean_target}")
                            return amt, side
    except Exception as e:
        print(f"[POSITION DETAIL CHECK ERROR]: {e}")

    return 0.0, None


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
            timeout=5
        )
        print(f">>> [MARKET FLATTEN RESULT HTTP {resp.status_code}]: {resp.text.strip()}")
    except Exception as e:
        print(f"[EMERGENCY CLOSE ERROR]: {e}")


def close_position_immediately(symbol: str):
    """Condition 7 & 13: Flattens open position before opening an opposing position."""
    size, side = get_active_position_details(symbol)
    if size > 0 and side:
        close_side = "SELL" if side == "BUY" else "BUY"
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        print(f"[AUTO-REVERSAL FLATTEN] Liquidating existing {side} ({size} BTC) via {close_side} MARKET order...")
        emergency_market_close(clean_target, close_side, size)
        time.sleep(1.5)


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Condition 3: Watchdog thread to market close position if price blows through the stop limit."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE
    is_buying_back = side == "BUY"
    clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()

    while sl_client_id in ACTIVE_SL_CLIENT_IDS and CURRENT_TRADE_ID == trade_id:
        time.sleep(1.0)
        curr_price = get_current_market_price(clean_target)
        if curr_price <= 0.0:
            continue

        spike_short_breached = is_buying_back and (curr_price > limit_price)
        spike_long_breached = (not is_buying_back) and (curr_price < limit_price)

        if spike_short_breached or spike_long_breached:
            print(f"\n[SPIKE DETECTED] Price ({curr_price}) breached limit ceiling ({limit_price})!")
            with ORDER_EXECUTION_LOCK:
                size, _ = get_active_position_details(clean_target)
                if size > 0:
                    print(f"[EMERGENCY ACTIVATED] Liquidating immediately via Market Order...")
                    cancel_all_tracked_stops()
                    emergency_market_close(clean_target, side, quantity)
                    CURRENT_POSITION_SIDE = None
                else:
                    print(f"[MONITOR] Position already closed. Watchdog standing down.")
            break


def get_actual_fill_price(client_order_id: str, symbol: str, fallback_price: float) -> float:
    """
    Condition 14: Queries Shark Exchange to retrieve the exact executed fill price.
    Checks live positions first to eliminate reporting lag and avoid fallback warnings.
    """
    clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
    variants = get_symbol_variants(symbol)

    for attempt in range(8):
        time.sleep(1.2)
        try:
            ts = str(int(time.time() * 1000))

            # 1. Primary Check: Live positions entry price
            for sym_var in variants + [""]:
                q_pos = f"symbol={sym_var}&timestamp={ts}" if sym_var else f"timestamp={ts}"
                headers_pos = get_headers(q_pos)
                resp_pos = requests.get(f"{SHARK_BASE_URL}/v1/positions?{q_pos}", headers=headers_pos, timeout=4)
                if resp_pos.status_code == 200:
                    pos_data = resp_pos.json()
                    positions = pos_data.get("data", pos_data) if isinstance(pos_data, dict) else pos_data
                    if isinstance(positions, list):
                        for pos in positions:
                            raw_sym = str(pos.get("symbol", "")).replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
                            if raw_sym == clean_target:
                                entry_p = float(pos.get("entryPrice") or pos.get("avgPrice") or pos.get("avgCost") or pos.get("price") or 0.0)
                                if entry_p > 0:
                                    print(f">>> [FOUND REAL ENTRY VIA POSITION] Position entry price: {entry_p}")
                                    return entry_p

            # 2. Secondary Check: Order detail by clientOrderId
            for sym_var in variants:
                q_detail = f"clientOrderId={client_order_id}&symbol={sym_var}&timestamp={ts}"
                headers_detail = get_headers(q_detail)
                resp_detail = requests.get(f"{SHARK_BASE_URL}/v1/order/order-detail?{q_detail}", headers=headers_detail, timeout=4)
                if resp_detail.status_code == 200:
                    detail_data = resp_detail.json()
                    data = detail_data.get("data", detail_data) if isinstance(detail_data, dict) else detail_data
                    if isinstance(data, dict):
                        exec_price = float(data.get("avgPrice") or data.get("price") or data.get("executedPrice") or 0.0)
                        if exec_price > 0:
                            print(f">>> [FOUND REAL ENTRY VIA ORDER-DETAIL] Executed price: {exec_price}")
                            return exec_price

            # 3. Tertiary Check: Trade history
            for sym_var in variants:
                query_str2 = f"symbol={sym_var}&timestamp={ts}"
                headers2 = get_headers(query_str2)
                resp2 = requests.get(f"{SHARK_BASE_URL}/v1/order/trade-history?{query_str2}", headers=headers2, timeout=4)
                if resp2.status_code == 200:
                    trades_data = resp2.json()
                    trades = trades_data.get("data", trades_data) if isinstance(trades_data, dict) else trades_data
                    if isinstance(trades, list) and len(trades) > 0:
                        for trade in trades:
                            t_cid = str(trade.get("clientOrderId", "") or trade.get("orderId", ""))
                            if client_order_id in t_cid or t_cid in client_order_id:
                                trade_price = float(trade.get("price") or trade.get("executedPrice") or 0.0)
                                if trade_price > 0:
                                    print(f">>> [FOUND REAL ENTRY VIA TRADE-HISTORY] Trade price: {trade_price}")
                                    return trade_price

        except Exception as e:
            print(f"[FETCH ATTEMPT {attempt + 1} ERROR]: {e}")

    print(f"[WARN] Could not resolve exact fill price from exchange, falling back to: {fallback_price}")
    return fallback_price


def delete_single_order(client_order_id: str) -> bool:
    """
    Conditions 6 & 7: Cancels an order using the exact payload schema required by Shark Exchange.
    Omits 'symbol' completely to eliminate HTTP 400 and HMAC 403 errors.
    """
    ts = str(int(time.time() * 1000))

    # Method 1: JSON body DELETE (clean payload without 'symbol' field)
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
                timeout=5
            )
            print(f"[CLEANUP BODY] Cancel ({client_order_id}) -> HTTP {resp_b.status_code} | {resp_b.text.strip()}")
            if resp_b.status_code in [200, 201, 204]:
                return True
        except Exception as e:
            print(f"[CLEANUP BODY ERROR]: {e}")

    # Method 2: Query String DELETE fallback
    query_variants = [
        f"clientOrderId={client_order_id}&timestamp={ts}",
        f"orderId={client_order_id}&timestamp={ts}"
    ]

    for query_str in query_variants:
        try:
            headers_q = get_headers(query_str)
            resp_q = requests.delete(
                f"{SHARK_BASE_URL}/v1/order/delete-order?{query_str}",
                headers=headers_q,
                timeout=5
            )
            print(f"[CLEANUP QUERY] Cancel ({client_order_id}) -> HTTP {resp_q.status_code} | {resp_q.text.strip()}")
            if resp_q.status_code in [200, 201, 204]:
                return True
        except Exception as e:
            print(f"[CLEANUP QUERY ERROR]: {e}")

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


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0) -> str:
    """Condition 3: Submits a STOP_LIMIT order with a 15 pt limit buffer."""
    global ACTIVE_SL_CLIENT_IDS, CURRENT_TRADE_ID
    try:
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()

        if ref_price <= 0 or (side == "SELL" and stop_price >= ref_price) or (side == "BUY" and stop_price <= ref_price):
            live_mkt = get_current_market_price(clean_target)
            if live_mkt > 0:
                print(f"[GUARD CHECK] Verified ref_price using live market ticker: {live_mkt}")
                ref_price = live_mkt

        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[GUARD] Long SL {stop_price} >= Market Price {ref_price}. Skipping placement.")
                return None
            if side == "BUY" and stop_price <= ref_price:
                print(f"[GUARD] Short SL {stop_price} <= Market Price {ref_price}. Skipping placement.")
                return None

        # 15 pt buffer logic
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
            timeout=5
        )

        if resp.status_code in [200, 201]:
            res_data = resp.json()
            cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
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
    """Condition 6 & 14: Monitors limit order fill for 600s, then sets 100 pt SL from real fill price."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, ACTIVE_ENTRY_ORDER_ID, ACTIVE_SL_CLIENT_IDS
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling Shark Exchange for {target_side} fill (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
        time.sleep(2.0)
        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded. Exiting watcher.")
            return

        if not ACTIVE_ENTRY_ORDER_ID:
            return

        still_open = is_entry_order_open(ACTIVE_ENTRY_ORDER_ID, clean_symbol)

        if not still_open:
            # Verify position or filled trade before placing stop
            pos_size, _ = get_active_position_details(clean_symbol)
            if pos_size > 0 or not is_entry_order_open(ACTIVE_ENTRY_ORDER_ID, clean_symbol):
                print(f">>> [LIMIT FILLED] Fetching true executed fill price...")
                CURRENT_POSITION_SIDE = target_side

                real_fill_price = get_actual_fill_price(ACTIVE_ENTRY_ORDER_ID, clean_symbol, entry_price)
                print(f">>> [EXECUTION CONFIRMED] Real Entry Price: {real_fill_price}")

                initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
                sl_side = "SELL" if is_buy else "BUY"

                new_sl_id = place_stop_loss(clean_symbol, sl_side, quantity, initial_stop, real_fill_price)
                if new_sl_id:
                    ACTIVE_SL_CLIENT_IDS.append(new_sl_id)
                return

    # Timeout reached without execution: cancel limit entry order
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

            # 1. Purge prior orders
            cancel_pending_limit_entry()
            cancel_all_tracked_stops()

            # 2. Close prior position before reversing (Condition 7 & 13)
            close_position_immediately(clean_symbol)

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
                timeout=5
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
    Conditions 4, 5, 11, 12, 13:
    Replaces trailing stops using verified exchange state and runtime memory fallback.
    """
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_SL_CLIENT_IDS
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

            # 1. Query Shark Exchange positions
            pos_size, live_side = get_active_position_details(clean_symbol)

            resolved_side = None
            if pos_size > 0 and live_side is not None:
                resolved_side = live_side
                CURRENT_POSITION_SIDE = live_side
            elif CURRENT_POSITION_SIDE is not None:
                print(f"[FALLBACK ENGAGED] Exchange API query returned 0, but bot has active {CURRENT_POSITION_SIDE} in memory.")
                resolved_side = CURRENT_POSITION_SIDE
            elif fallback_side in ["BUY", "SELL"] and CURRENT_TRADE_ID == trade_id:
                print(f"[FALLBACK ENGAGED] Restoring from webhook trade side: {fallback_side}")
                resolved_side = fallback_side
                CURRENT_POSITION_SIDE = fallback_side

            # Guard: If no trade exists anywhere, discard (Condition 11)
            if resolved_side is None:
                print(f"[REJECTED UPDATE_SL] No open trade found on exchange or in bot state. Discarding trailing SL.")
                return

            CURRENT_TRADE_ID = trade_id
            clean_qty = float(f"{quantity:.4f}")
            stop_price = float(f"{sl_price:.2f}")
            ref_price = float(f"{current_price:.2f}") if current_price else 0.0

            if stop_price <= 0:
                print(f"[UPDATE_SL] Invalid stop price {stop_price}. Skipped.")
                return

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
