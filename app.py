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
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip().strip("'").strip('"')

# Condition 6 & 10: Configurable timeout duration for limit entries
ENTRY_ORDER_EXPIRATION_SECONDS = 600

CURRENT_TRADE_ID = None
CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []
ACTIVE_ENTRY_ORDER_ID = None

ORDER_EXECUTION_LOCK = threading.Lock()


def get_headers(payload_or_querystr: str) -> dict:
    """Computes HMAC-SHA256 signature strictly on the exact UTF-8 string payload."""
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
    """Checks whether an entry order is still resting on the order book."""
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
    """Fetches the latest live market price."""
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


def get_active_position_details(symbol: str):
    """
    Queries Shark Exchange positions using multiple lookup formats.
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
                pos_data = resp.json()
                positions = pos_data.get("data", pos_data) if isinstance(pos_data, dict) else pos_data
                if isinstance(positions, list):
                    for pos in positions:
                        raw_sym = str(pos.get("symbol", "")).replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
                        if raw_sym == clean_target:
                            amt = float(pos.get("positionAmt") or pos.get("size") or pos.get("contractVal") or 0.0)
                            if amt != 0:
                                pos_side = "BUY" if amt > 0 else "SELL"
                                print(f">>> [POSITION DETECTED] Found active {pos_side} position: {abs(amt)} ({pos.get('symbol')})")
                                return abs(amt), pos_side
    except Exception as e:
        print(f"[POSITION DETAIL CHECK ERROR]: {e}")
    return 0.0, None


def emergency_market_close(symbol: str, side: str, quantity: float):
    """Executes an immediate market order with reduceOnly=True."""
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
    """Liquidates any existing open position immediately via Market order (Condition 7)."""
    size, side = get_active_position_details(symbol)
    if size > 0 and side:
        close_side = "SELL" if side == "BUY" else "BUY"
        clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
        print(f"[AUTO-REVERSAL FLATTEN] Closing prior {side} position ({size} BTC) via {close_side} MARKET order...")
        emergency_market_close(clean_target, close_side, size)
        time.sleep(1.5)


def monitor_slippage_and_market_close(symbol: str, side: str, quantity: float, limit_price: float, sl_client_id: str, trade_id: int):
    """Monitors price while STOP_LIMIT is resting to sweep via market if blown past (Condition 3)."""
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
                    cancel_all_tracked_stops(clean_target)
                    emergency_market_close(clean_target, side, quantity)
                    CURRENT_POSITION_SIDE = None
                else:
                    print(f"[MONITOR] Position already filled. Standing down.")
            break


def get_actual_fill_price(client_order_id: str, symbol: str, fallback_price: float) -> float:
    """Queries order details, positions, and trade history to capture the real filled entry price."""
    clean_target = symbol.replace("-", "").replace("_", "").replace(".P", "").replace("/", "").upper()
    variants = get_symbol_variants(symbol)

    for attempt in range(5):
        time.sleep(1.0)
        try:
            ts = str(int(time.time() * 1000))

            # 1. Primary Check: Query order detail by clientOrderId
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

            # 2. Secondary Check: Live positions average price
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


def delete_single_order(client_order_id: str, symbol: str = "BTC-USDT") -> bool:
    """Cancels an order targeting both BTC-USDT and BTCUSDT formats across query strings and JSON payloads."""
    success = False
    variants = get_symbol_variants(symbol)
    ts = str(int(time.time() * 1000))

    for sym_candidate in variants:
        try:
            # Method 1: Query string DELETE
            query_str = f"clientOrderId={client_order_id}&symbol={sym_candidate}&timestamp={ts}"
            headers_q = get_headers(query_str)
            resp_q = requests.delete(
                f"{SHARK_BASE_URL}/v1/order/delete-order?{query_str}",
                headers=headers_q,
                timeout=5
            )
            print(f"[CLEANUP QUERY {sym_candidate}] Cancel ({client_order_id}) -> HTTP {resp_q.status_code} | {resp_q.text.strip()}")
            if resp_q.status_code in [200, 201, 204]:
                success = True

            # Method 2: JSON Body DELETE
            payload = {"clientOrderId": str(client_order_id), "symbol": sym_candidate, "timestamp": ts}
            body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
            headers_b = get_headers(body)
            resp_b = requests.delete(
                f"{SHARK_BASE_URL}/v1/order/delete-order",
                data=body.encode("utf-8"),
                headers=headers_b,
                timeout=5
            )
            print(f"[CLEANUP BODY {sym_candidate}] Cancel ({client_order_id}) -> HTTP {resp_b.status_code} | {resp_b.text.strip()}")
            if resp_b.status_code in [200, 201, 204]:
                success = True

        except Exception as e:
            print(f"[CLEANUP ATTEMPT ERROR {sym_candidate}]: {e}")

    return success


def cancel_all_tracked_stops(symbol: str = "BTC-USDT"):
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid, symbol)
    ACTIVE_SL_CLIENT_IDS = []


def cancel_pending_limit_entry(symbol: str = "BTC-USDT"):
    global ACTIVE_ENTRY_ORDER_ID
    if ACTIVE_ENTRY_ORDER_ID:
        print(f"[ENTRY CLEANUP] Canceling pending limit entry: {ACTIVE_ENTRY_ORDER_ID}")
        delete_single_order(ACTIVE_ENTRY_ORDER_ID, symbol)
        ACTIVE_ENTRY_ORDER_ID = None


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0) -> str:
    """Submits a STOP_LIMIT order with a 15 pt limit buffer (Condition 3)."""
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
    """Monitors limit order execution with an automatic 600s cutoff (Condition 6)."""
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE, ACTIVE_ENTRY_ORDER_ID, ACTIVE_SL_CLIENT_IDS
    is_buy = target_side == "BUY"
    start_time = time.time()

    print(f"[WATCHER] Polling Shark Exchange for {target_side} fill (Timeout: {ENTRY_ORDER_EXPIRATION_SECONDS}s)...")

    while (time.time() - start_time) < ENTRY_ORDER_EXPIRATION_SECONDS:
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

            real_fill_price = get_actual_fill_price(ACTIVE_ENTRY_ORDER_ID, clean_symbol, entry_price)
            print(f">>> [EXECUTION CONFIRMED] Real Entry Price: {real_fill_price}")

            # 100 pt initial stop from real fill price (Condition 3)
            initial_stop = round(real_fill_price - 100.0, 2) if is_buy else round(real_fill_price + 100.0, 2)
            sl_side = "SELL" if is_buy else "BUY"

            new_sl_id = place_stop_loss(clean_symbol, sl_side, quantity, initial_stop, real_fill_price)
            if new_sl_id:
                ACTIVE_SL_CLIENT_IDS.append(new_sl_id)
            return

    print(f"[WATCHER] Limit order timed out after {ENTRY_ORDER_EXPIRATION_SECONDS}s without filling. Canceling order...")
    with ORDER_EXECUTION_LOCK:
        cancel_pending_limit_entry(clean_symbol)
        CURRENT_POSITION_SIDE = None


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
            clean_price = float(f"{target_limit_price:.2f}")
            clean_qty = float(f"{quantity:.4f}")

            # 1. Purge prior resting orders (Conditions 6 & 7)
            cancel_pending_limit_entry(clean_symbol)
            cancel_all_tracked_stops(clean_symbol)

            # 2. Flatten opposite active positions before reversing (Condition 7)
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
    """Replaces older stop orders with newly confirmed swing/profit lock levels (Conditions 4, 5, 11, 12)."""
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_SL_CLIENT_IDS
    with ORDER_EXECUTION_LOCK:
        try:
            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

            print(f"[UPDATE_SL] Requested SL: {sl_price} | Position Side: {CURRENT_POSITION_SIDE} | Fallback Side: {fallback_side} | TradeID: {trade_id}")

            CURRENT_TRADE_ID = trade_id
            clean_qty = float(f"{quantity:.4f}")
            stop_price = float(f"{sl_price:.2f}")
            ref_price = float(f"{current_price:.2f}") if current_price else 0.0

            # 1. Recover position side from webhook payload (Condition 11)
            if CURRENT_POSITION_SIDE is None and fallback_side in ["BUY", "SELL"]:
                CURRENT_POSITION_SIDE = fallback_side
                print(f"[RECOVERY] Restored Position Side from alert: {CURRENT_POSITION_SIDE}")

            # 2. Recover from live exchange endpoint
            if CURRENT_POSITION_SIDE is None:
                size, live_side = get_active_position_details(clean_symbol)
                if live_side:
                    CURRENT_POSITION_SIDE = live_side
                    print(f"[RECOVERY] Restored Position Side from exchange: {CURRENT_POSITION_SIDE}")

            if CURRENT_POSITION_SIDE is None:
                print(f"[REJECTED UPDATE_SL] No open position known or found on exchange. Discarding trail.")
                return

            if stop_price <= 0:
                print(f"[UPDATE_SL] Invalid stop price {stop_price}. Skipped.")
                return

            sl_side = "SELL" if CURRENT_POSITION_SIDE == "BUY" else "BUY"

            # 3. Place new STOP_LIMIT first
            print(f"[UPDATE_SL] Submitting new {sl_side} Stop to {stop_price}...")
            new_sl_id = place_stop_loss(clean_symbol, sl_side, clean_qty, stop_price, ref_price)

            # 4. Cancel old stops only if the new one was accepted
            if new_sl_id:
                old_stops = [cid for cid in ACTIVE_SL_CLIENT_IDS if cid != new_sl_id]
                for old_cid in old_stops:
                    delete_single_order(old_cid, clean_symbol)
                ACTIVE_SL_CLIENT_IDS = [new_sl_id]
                print(f">>> [TRAILING SL UPDATED] Active stop is now {new_sl_id} at {stop_price}")
            else:
                print(f"[WARN] New SL placement failed or was skipped. Keeping existing protective stops intact.")

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
    # Condition 8: Read dynamic trade quantity from payload
    quantity = float(data.get("quantity", 0.002))
    target_limit_price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))

    # Condition 11: Read position_side from payload to prevent state desync
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
