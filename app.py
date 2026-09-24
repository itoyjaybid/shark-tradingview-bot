import os
import time
import json
import hmac
import hashlib
import socket
import requests
import threading
from email.utils import parsedate_to_datetime
from datetime import datetime
from contextlib import asynccontextmanager

import urllib3.util.connection as urllib3_cn
from fastapi import FastAPI, Request, HTTPException

# =============================================================================
# FORCE IPV4
# =============================================================================

def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

# =============================================================================
# CONFIGURATION
# =============================================================================

SHARK_BASE_URL = "https://api.sharkexchange.in"

SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip().strip("'").strip('"')
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip().strip("'").strip('"')

WEBHOOK_PASSPHRASE = os.getenv(
    "WEBHOOK_PASSPHRASE",
    "MY_SECRET_KEY"
).strip().strip("'").strip('"')

DEFAULT_SYMBOL = "BTCUSDT"
STATE_FILE = "state.json"

ENGINE_LOCK = threading.RLock()
TS_LOCK = threading.Lock()

LAST_USED_TIMESTAMP = 0
CLOCK_OFFSET_MS = 0

INITIAL_SL_POINTS = 100.0
STOP_LIMIT_BUFFER = 15.0

# =============================================================================
# BOT STATE
# =============================================================================

BOT_STATE = {
    "trade_id": None,
    "side": None,
    "qty": 0.0,
    "entry_id": None,
    "sl_id": None,
    "fill_price": 0.0,
    "sl_price": 0.0,
    "entry_active": False,
    "position_active": False
}

SL_WATCH_LOCK = threading.Lock()
SL_WATCH = {}

ORDER_STATUS_FAIL_LOGGED = set()

POSITION_RAW_LOG_LOCK = threading.Lock()
LAST_POSITION_RAW_LOG = 0.0
POSITION_RAW_LOG_INTERVAL_SEC = 8.0

PENDING_SL_LOCK = threading.Lock()
PENDING_SL = {}
RETRY_THREAD_ACTIVE = set()

def register_sl_watch(sl_id: str, stop_price: float, limit_price: float, side: str, qty: float):
    with SL_WATCH_LOCK:
        SL_WATCH[sl_id] = {
            "stop": stop_price,
            "limit": limit_price,
            "side": side,
            "qty": qty
        }

def update_sl_watch(sl_id: str, stop_price: float = None, limit_price: float = None, qty: float = None):
    with SL_WATCH_LOCK:
        entry = SL_WATCH.get(sl_id)
        if not entry:
            return
        if stop_price is not None:
            entry["stop"] = stop_price
        if limit_price is not None:
            entry["limit"] = limit_price
        if qty is not None:
            entry["qty"] = qty

def get_sl_watch(sl_id: str):
    with SL_WATCH_LOCK:
        entry = SL_WATCH.get(sl_id)
        return dict(entry) if entry else None

def remove_sl_watch(sl_id: str):
    if not sl_id:
        return
    with SL_WATCH_LOCK:
        SL_WATCH.pop(sl_id, None)

# =============================================================================
# STATE PERSISTENCE
# =============================================================================

def load_state():
    global BOT_STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                loaded = json.load(f)
            BOT_STATE.update(loaded)
            print(
                f"[STATE LOADED] "
                f"TradeID={BOT_STATE.get('trade_id')} "
                f"Side={BOT_STATE.get('side')} "
                f"Qty={BOT_STATE.get('qty')}",
                flush=True
            )
        except Exception as e:
            print(f"[STATE LOAD ERROR] {e}", flush=True)

def save_state():
    try:
        temp_file = STATE_FILE + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(BOT_STATE, f, indent=2)
        os.replace(temp_file, STATE_FILE)
    except Exception as e:
        print(f"[STATE SAVE ERROR] {e}", flush=True)

def purge_state():
    old_sl_id = BOT_STATE.get("sl_id")
    BOT_STATE["trade_id"] = None
    BOT_STATE["side"] = None
    BOT_STATE["qty"] = 0.0
    BOT_STATE["entry_id"] = None
    BOT_STATE["sl_id"] = None
    BOT_STATE["fill_price"] = 0.0
    BOT_STATE["sl_price"] = 0.0
    BOT_STATE["entry_active"] = False
    BOT_STATE["position_active"] = False
    save_state()
    remove_sl_watch(old_sl_id)

# =============================================================================
# CLOCK & AUTHENTICATION
# =============================================================================

def sync_clock_directly() -> int:
    global CLOCK_OFFSET_MS
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/market/ticker24Hr/btcusdt", timeout=2)
        if "Date" in r.headers:
            server_dt = parsedate_to_datetime(r.headers["Date"])
            server_ts = int(server_dt.timestamp() * 1000)
            local_ts = int(time.time() * 1000)
            CLOCK_OFFSET_MS = server_ts - local_ts
            return server_ts
    except Exception as e:
        print(f"[CLOCK SYNC ERROR] {e}", flush=True)

    return int(time.time() * 1000) + CLOCK_OFFSET_MS

def update_clock_from_server_time(iso_time_str: str):
    global CLOCK_OFFSET_MS
    if not iso_time_str:
        return
    try:
        cleaned = iso_time_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        srv_ts = int(dt.timestamp() * 1000)
        local_ts = int(time.time() * 1000)
        new_offset = srv_ts - local_ts
        if abs(new_offset - CLOCK_OFFSET_MS) > 200:
            print(
                f"[CLOCK SYNC] Offset updated from response: "
                f"{CLOCK_OFFSET_MS}ms -> {new_offset}ms",
                flush=True
            )
        CLOCK_OFFSET_MS = new_offset
    except Exception as e:
        print(f"[CLOCK SYNC FROM ORDER ERROR] {e}", flush=True)

def get_synced_time() -> int:
    global LAST_USED_TIMESTAMP
    with TS_LOCK:
        now_ts = int(time.time() * 1000) + CLOCK_OFFSET_MS
        if now_ts <= LAST_USED_TIMESTAMP:
            now_ts = LAST_USED_TIMESTAMP + 1
        LAST_USED_TIMESTAMP = now_ts
        return now_ts

def generate_signature(api_secret: str, data_to_sign: str) -> str:
    return hmac.new(
        api_secret.encode("utf-8"),
        data_to_sign.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def sign_data(data_str: str) -> dict:
    sig = generate_signature(SHARK_API_SECRET, data_str)
    return {
        "api-key": SHARK_API_KEY,
        "signature": sig,
        "Content-Type": "application/json"
    }

def clean_symbol(sym: str) -> str:
    if not sym:
        return DEFAULT_SYMBOL
    return (
        sym
        .replace("-", "")
        .replace("_", "")
        .replace(".P", "")
        .replace(".p", "")
        .replace("/", "")
        .upper()
    )

# =============================================================================
# ORDER OPERATIONS
# =============================================================================

def send_signed_order(payload: dict, max_retries: int = 3):
    for attempt in range(max_retries):
        if attempt > 0:
            sync_clock_directly()
            time.sleep(0.15)

        payload["timestamp"] = str(get_synced_time())
        data_to_sign = json.dumps(payload, separators=(',', ':'))
        headers = sign_data(data_to_sign)

        try:
            r = requests.post(
                f"{SHARK_BASE_URL}/v1/order/place-order",
                data=data_to_sign,
                headers=headers,
                timeout=4
            )

            if r.status_code in [200, 201]:
                res = r.json()
                cid = res.get("clientOrderId") or res.get("data", {}).get("clientOrderId")
                print(f"[ORDER SUCCESS] {res}", flush=True)
                update_clock_from_server_time(res.get("time"))
                return True, cid

            if r.status_code == 403 or "4007" in r.text or "Signature mismatch" in r.text:
                print(
                    f"[SIGNATURE RETRY] Attempt {attempt + 1} -> "
                    f"HTTP {r.status_code}: {r.text} | "
                    f"timestamp_used={payload.get('timestamp')}",
                    flush=True
                )
                continue

            print(f"[EXCHANGE REJECTED] HTTP {r.status_code}: {r.text}", flush=True)
            return False, None

        except Exception as e:
            print(f"[DISPATCH ERROR] {e}", flush=True)
            time.sleep(0.10)

    print(f"[SIGNATURE RETRY EXHAUSTED] All {max_retries} attempts failed.", flush=True)
    return False, None

def cancel_order(client_order_id: str) -> bool:
    if not client_order_id:
        return True

    params = {
        "clientOrderId": str(client_order_id),
        "timestamp": str(get_synced_time())
    }

    data_to_sign = json.dumps(params, separators=(',', ':'))
    headers = sign_data(data_to_sign)

    try:
        r = requests.delete(
            f"{SHARK_BASE_URL}/v1/order/delete-order",
            data=data_to_sign,
            headers=headers,
            timeout=4
        )

        if r.status_code in [200, 201, 204]:
            print(f"[ORDER CANCELLED] {client_order_id}", flush=True)
            return True

        print(f"[CANCEL FAILED] {r.status_code}: {r.text}", flush=True)
        return False
    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)
        return False

def edit_stop_order(
    client_order_id: str,
    quantity: float,
    stop_price: float,
    limit_price: float
) -> bool:
    if not client_order_id:
        return False

    params = {
        "clientOrderId": str(client_order_id),
        "timestamp": str(get_synced_time()),
        "quantity": float(f"{quantity:.4f}"),
        "stopPrice": float(f"{stop_price:.2f}"),
        "price": float(f"{limit_price:.2f}")
    }

    data_to_sign = json.dumps(params, separators=(',', ':'))
    headers = sign_data(data_to_sign)

    try:
        r = requests.patch(
            f"{SHARK_BASE_URL}/v1/order/edit-order",
            data=data_to_sign,
            headers=headers,
            timeout=4
        )

        if r.status_code in [200, 201]:
            print(f"[STOP EDITED] ID={client_order_id} Stop={stop_price} Limit={limit_price}", flush=True)
            return True

        print(f"[STOP EDIT FAILED] {r.status_code}: {r.text}", flush=True)
    except Exception as e:
        print(f"[STOP EDIT ERROR] {e}", flush=True)

    return False

# =============================================================================
# POSITION & MARKET DATA
# =============================================================================

def get_exchange_position_state(symbol: str):
    target = clean_symbol(symbol)
    ts = str(get_synced_time())

    params = {
        "sortOrder": "desc",
        "pageSize": "100",
        "symbol": target,
        "timestamp": ts
    }
    query_string = "&".join(f"{k}={v}" for k, v in params.items())
    headers = {
        "api-key": SHARK_API_KEY,
        "signature": generate_signature(SHARK_API_SECRET, query_string),
        "accept": "*/*"
    }

    endpoint = f"/v1/positions/OPEN?{query_string}"

    try:
        r = requests.get(f"{SHARK_BASE_URL}{endpoint}", headers=headers, timeout=3)
        if r.status_code != 200:
            print(f"[POSITION CHECK FAILED] HTTP {r.status_code}: {r.text}", flush=True)
            return 0.0, "FLAT", 0.0

        res = r.json()

        global LAST_POSITION_RAW_LOG
        now = time.time()
        with POSITION_RAW_LOG_LOCK:
            should_log = (now - LAST_POSITION_RAW_LOG) >= POSITION_RAW_LOG_INTERVAL_SEC
            if should_log:
                LAST_POSITION_RAW_LOG = now

        if should_log:
            print(f"[POSITION RAW] target={target} -> {res}", flush=True)

        items = res if isinstance(res, list) else res.get("data", [])
        if isinstance(items, dict):
            items = [items]

        for pos in items:
            if not isinstance(pos, dict):
                continue

            sym = str(pos.get("contractPair") or pos.get("symbol") or "").upper()
            if target not in sym and sym not in target:
                continue

            raw_qty = float(
                pos.get("positionAmount")
                or pos.get("quantity")
                or pos.get("positionAmt")
                or 0.0
            )
            position_type = str(
                pos.get("positionType")
                or pos.get("side")
                or ""
            ).upper()
            entry_price = float(pos.get("entryPrice") or 0.0)

            if abs(raw_qty) > 0.000001:
                if position_type in ["SHORT", "SELL"] or raw_qty < 0:
                    return abs(raw_qty), "SELL", entry_price
                return abs(raw_qty), "BUY", entry_price

        return 0.0, "FLAT", 0.0
    except Exception as e:
        print(f"[POSITION CHECK ERROR] {e}", flush=True)
        return 0.0, "FLAT", 0.0

def wait_until_flat(symbol: str, timeout_sec: float = 5.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        qty, _, _ = get_exchange_position_state(symbol)
        if qty <= 0.000001:
            return True
        time.sleep(0.20)
    qty, _, _ = get_exchange_position_state(symbol)
    return qty <= 0.000001

def get_current_ticker_price(symbol: str) -> float:
    pair = clean_symbol(symbol).lower()
    try:
        r = requests.get(f"{SHARK_BASE_URL}/v1/market/ticker24Hr/{pair}", timeout=3)
        if r.status_code == 200:
            res = r.json()
            data = res.get("data", res)
            if isinstance(data, dict):
                price = data.get("c") or data.get("price") or data.get("lastPrice")
                if price is not None:
                    return float(price)
    except Exception as e:
        print(f"[TICKER CHECK ERROR] {pair} -> {e}", flush=True)
    return 0.0

def check_order_status(client_order_id: str, symbol: str):
    """
    Queries POST /v1/order/get-multiple.
    Accepts 200 OK and 201 Created.
    Extracts status, leveragedQty, cumQty, and avgPrice.
    """
    payload = {
        "clientOrderIds": [str(client_order_id)],
        "timestamp": str(get_synced_time())
    }
    data_to_sign = json.dumps(payload, separators=(',', ':'))
    headers = sign_data(data_to_sign)

    try:
        r = requests.post(
            f"{SHARK_BASE_URL}/v1/order/get-multiple",
            data=data_to_sign,
            headers=headers,
            timeout=3
        )
        if r.status_code not in [200, 201]:
            if client_order_id not in ORDER_STATUS_FAIL_LOGGED:
                print(
                    f"[ORDER STATUS FAILED] ID={client_order_id} -> HTTP {r.status_code}: {r.text}",
                    flush=True
                )
                ORDER_STATUS_FAIL_LOGGED.add(client_order_id)
            return "UNKNOWN", 0.0

        orders = r.json()
        if isinstance(orders, list) and len(orders) > 0:
            order = orders[0]
            status = str(order.get("status") or "").upper()
            avg_price = float(order.get("avgPrice") or order.get("price") or 0.0)

            if status in ["CANCELED", "CANCELLED", "REJECTED", "EXPIRED"]:
                return "CANCELLED", -1.0
            if status in ["FILLED", "SUCCESS", "EXECUTED", "COMPLETE"]:
                return "FILLED", avg_price

            order_amount = float(
                order.get("leveragedQty")
                or order.get("quantity")
                or order.get("orderAmount")
                or 0.0
            )
            filled_amount = float(
                order.get("cumQty")
                or order.get("filledQty")
                or order.get("executedQty")
                or order.get("filledAmount")
                or 0.0
            )

            if order_amount > 0 and filled_amount >= order_amount * 0.999:
                return "FILLED", avg_price
            if filled_amount > 0:
                return "PARTIAL", avg_price

            # NEW or OPEN signifies an active resting limit order
            return "OPEN", 0.0
    except Exception as e:
        print(f"[ORDER STATUS ERROR] {e}", flush=True)

    return "UNKNOWN", 0.0

# =============================================================================
# STOP LOSS & EXECUTION FLOW
# =============================================================================

def place_stop_loss(symbol: str, side: str, qty: float, stop_price: float, trade_id: int):
    target = clean_symbol(symbol)
    offset = STOP_LIMIT_BUFFER

    if side == "SELL":
        limit_price = round(stop_price - offset, 2)
    else:
        limit_price = round(stop_price + offset, 2)

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
        "type": "STOP_LIMIT"
    }

    print(f"[STOP PLACE] {side} Stop={stop_price} Limit={limit_price}", flush=True)
    ok, cid = send_signed_order(payload)

    if ok and cid:
        print(f"[STOP PLACED] ID={cid}", flush=True)
        register_sl_watch(cid, stop_price, limit_price, side, qty)
        threading.Thread(
            target=monitor_stop_order,
            args=(target, cid, trade_id),
            daemon=True
        ).start()
        return cid

    return None

def monitor_stop_order(symbol: str, sl_id: str, trade_id: int):
    target = clean_symbol(symbol)
    start = time.time()

    while True:
        time.sleep(0.8)

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id or BOT_STATE.get("sl_id") != sl_id:
                remove_sl_watch(sl_id)
                return

        watch = get_sl_watch(sl_id)
        if watch is None:
            return

        limit_price = watch["limit"]
        sl_side = watch["side"]
        live_qty, _, _ = get_exchange_position_state(target)

        if live_qty <= 0.000001:
            with ENGINE_LOCK:
                if BOT_STATE.get("trade_id") == trade_id:
                    print("[STOP MONITOR] Position is FLAT. Clearing state.", flush=True)
                    purge_state()
            remove_sl_watch(sl_id)
            return

        curr_price = get_current_ticker_price(target)
        if curr_price <= 0:
            continue

        breached = (curr_price < limit_price) if sl_side == "SELL" else (curr_price > limit_price)
        if breached:
            print(f"[STOP GAP] Price={curr_price} Limit={limit_price}", flush=True)
            with ENGINE_LOCK:
                if BOT_STATE.get("sl_id") == sl_id and BOT_STATE.get("trade_id") == trade_id:
                    flatten_position(target, sl_side, live_qty)
                    if wait_until_flat(target, timeout_sec=5):
                        cancel_order(sl_id)
                        purge_state()
                    else:
                        print("[CRITICAL] Emergency flatten failed during stop gap.", flush=True)
                        remove_sl_watch(sl_id)
                        return

            remove_sl_watch(sl_id)
            return

        if time.time() - start > 86400:
            start = time.time()

def flatten_position(symbol: str, side: str, qty: float) -> bool:
    target = clean_symbol(symbol)
    if qty <= 0:
        return False

    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": float(f"{qty:.4f}"),
        "reduceOnly": True,
        "side": side,
        "symbol": target,
        "type": "MARKET"
    }

    print(f"[MARKET EXIT] {side} {qty} {target}", flush=True)
    ok, _ = send_signed_order(payload)
    return ok

def entry_order_watcher(symbol: str, side: str, limit_price: float, trade_id: int, qty: float, order_id: str, timeout_sec: int):
    target = clean_symbol(symbol)
    start = time.time()

    print(f"[ENTRY WATCHER] ID={order_id} Timeout={timeout_sec}s", flush=True)

    while time.time() - start < timeout_sec:
        time.sleep(0.8)

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id or BOT_STATE.get("entry_id") != order_id:
                return

        status, exec_price = check_order_status(order_id, target)
        if status == "CANCELLED":
            print("[ENTRY WATCHER] Entry cancelled/rejected.", flush=True)
            with ENGINE_LOCK:
                if BOT_STATE.get("entry_id") == order_id:
                    BOT_STATE["entry_id"] = None
                    BOT_STATE["entry_active"] = False
                    save_state()
            return

        filled_via_order_detail = status in ["FILLED", "PARTIAL"]
        filled_via_backstop = False

        if not filled_via_order_detail:
            backstop_qty, backstop_side, _ = get_exchange_position_state(target)
            if backstop_qty >= qty * 0.999 and backstop_side == side:
                filled_via_backstop = True

        if filled_via_order_detail or filled_via_backstop:
            live_qty, live_side, live_entry_price = get_exchange_position_state(target)
            if live_qty <= 0:
                continue

            real_fill = exec_price if exec_price > 0 else (live_entry_price if live_entry_price > 0 else limit_price)
            print(f"[ENTRY FILLED] {live_side} Qty={live_qty} Price={real_fill}", flush=True)

            with ENGINE_LOCK:
                if BOT_STATE.get("trade_id") != trade_id:
                    return

                BOT_STATE["side"] = live_side
                BOT_STATE["qty"] = live_qty
                BOT_STATE["fill_price"] = real_fill
                BOT_STATE["entry_id"] = None
                BOT_STATE["entry_active"] = False
                BOT_STATE["position_active"] = True

                if live_side == "BUY":
                    initial_stop = round(real_fill - INITIAL_SL_POINTS, 2)
                    sl_side = "SELL"
                else:
                    initial_stop = round(real_fill + INITIAL_SL_POINTS, 2)
                    sl_side = "BUY"

                sl_id = place_stop_loss(target, sl_side, live_qty, initial_stop, trade_id)
                if sl_id:
                    BOT_STATE["sl_id"] = sl_id
                    BOT_STATE["sl_price"] = initial_stop
                    save_state()
                else:
                    print("[CRITICAL] Initial SL failed. Exiting market.", flush=True)
                    flatten_position(target, sl_side, live_qty)
                    if wait_until_flat(target, 5):
                        purge_state()
                    return
            return

    # Timeout reached
    print(f"[ENTRY TIMEOUT] Cancelling {order_id}", flush=True)
    with ENGINE_LOCK:
        if BOT_STATE.get("trade_id") != trade_id:
            return

        cancel_ok = cancel_order(order_id)
        if not cancel_ok:
            live_qty, live_side, live_entry_price = get_exchange_position_state(target)
            if live_qty >= qty * 0.999 and live_side == side:
                real_fill = live_entry_price if live_entry_price > 0 else limit_price
                BOT_STATE["side"] = live_side
                BOT_STATE["qty"] = live_qty
                BOT_STATE["fill_price"] = real_fill
                BOT_STATE["entry_id"] = None
                BOT_STATE["entry_active"] = False
                BOT_STATE["position_active"] = True

                initial_stop = round(real_fill - INITIAL_SL_POINTS, 2) if live_side == "BUY" else round(real_fill + INITIAL_SL_POINTS, 2)
                sl_side = "SELL" if live_side == "BUY" else "BUY"
                sl_id = place_stop_loss(target, sl_side, live_qty, initial_stop, trade_id)
                if sl_id:
                    BOT_STATE["sl_id"] = sl_id
                    BOT_STATE["sl_price"] = initial_stop
                    save_state()
                return

        if BOT_STATE.get("entry_id") == order_id:
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False
            save_state()

def process_entry_signal(action: str, symbol: str, qty: float, price: float, trade_id: int, timeout_sec: int):
    target = clean_symbol(symbol)
    side = "BUY" if "BUY" in action else "SELL"

    with ENGINE_LOCK:
        current_id = BOT_STATE.get("trade_id")
        if current_id is not None and trade_id < current_id:
            print(f"[OLD SIGNAL IGNORED] {trade_id} < {current_id}", flush=True)
            return

        live_qty, live_side, _ = get_exchange_position_state(target)
        if live_qty > 0 and live_side != side:
            print(f"[REVERSAL] Closing {live_side} before {side}", flush=True)
            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)

            if not wait_until_flat(target, timeout_sec=5):
                print("[REVERSAL ABORTED] Could not confirm flat position.", flush=True)
                return

            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])
            purge_state()

        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False

        live_qty, live_side, _ = get_exchange_position_state(target)
        if live_qty > 0:
            print(f"[ENTRY BLOCKED] Existing position active: {live_side} {live_qty}", flush=True)
            return

        BOT_STATE["trade_id"] = trade_id
        BOT_STATE["side"] = side
        BOT_STATE["qty"] = qty
        BOT_STATE["entry_id"] = None
        BOT_STATE["sl_id"] = None
        BOT_STATE["fill_price"] = 0.0
        BOT_STATE["sl_price"] = 0.0
        BOT_STATE["entry_active"] = True
        BOT_STATE["position_active"] = False
        save_state()

        clean_price = float(f"{price:.2f}")
        clean_qty = float(f"{qty:.4f}")

        payload = {
            "deviceType": "WEB",
            "marginAsset": "INR",
            "placeType": "ORDER_FORM",
            "price": clean_price,
            "quantity": clean_qty,
            "reduceOnly": False,
            "side": side,
            "symbol": target,
            "type": "LIMIT"
        }

        print(f"[ENTRY] {side} {clean_qty} {target} @ {clean_price}", flush=True)
        ok, cid = send_signed_order(payload)

        if not ok or not cid:
            print("[ENTRY FAILED]", flush=True)
            purge_state()
            return

        BOT_STATE["entry_id"] = cid
        save_state()

        threading.Thread(
            target=entry_order_watcher,
            args=(target, side, clean_price, trade_id, clean_qty, cid, timeout_sec),
            daemon=True
        ).start()

# =============================================================================
# TRAILING & EXIT LOGIC
# =============================================================================

def attempt_stop_update(target: str, sl_side: str, live_qty: float, new_stop: float, trade_id: int) -> bool:
    old_sl_id = BOT_STATE.get("sl_id")
    if old_sl_id:
        offset = STOP_LIMIT_BUFFER
        new_limit = round(new_stop - offset, 2) if sl_side == "SELL" else round(new_stop + offset, 2)

        if edit_stop_order(old_sl_id, live_qty, new_stop, new_limit):
            update_sl_watch(old_sl_id, stop_price=new_stop, limit_price=new_limit, qty=live_qty)
            BOT_STATE["qty"] = live_qty
            BOT_STATE["sl_price"] = new_stop
            save_state()
            print(f"[STOP UPDATED] {new_stop}", flush=True)
            return True

        new_sl_id = place_stop_loss(target, sl_side, live_qty, new_stop, trade_id)
        if new_sl_id:
            cancel_order(old_sl_id)
            remove_sl_watch(old_sl_id)
            BOT_STATE["sl_id"] = new_sl_id
            BOT_STATE["sl_price"] = new_stop
            save_state()
            print(f"[STOP UPDATED] {new_stop}", flush=True)
            return True
        return False

    new_sl_id = place_stop_loss(target, sl_side, live_qty, new_stop, trade_id)
    if new_sl_id:
        BOT_STATE["sl_id"] = new_sl_id
        BOT_STATE["sl_price"] = new_stop
        save_state()
        print(f"[STOP UPDATED] {new_stop}", flush=True)
        return True
    return False

def sl_update_retry_loop(symbol: str, trade_id: int):
    target = clean_symbol(symbol)
    backoffs = [2, 3, 5, 8, 13, 20, 30]
    attempt = 0

    while True:
        wait_s = backoffs[min(attempt, len(backoffs) - 1)]
        attempt += 1
        time.sleep(wait_s)

        with PENDING_SL_LOCK:
            pending_stop = PENDING_SL.get(trade_id)
        if pending_stop is None:
            break

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id:
                break

            live_qty, live_side, _ = get_exchange_position_state(target)
            if live_qty <= 0:
                purge_state()
                break

            sl_side = "SELL" if live_side == "BUY" else "BUY"
            success = attempt_stop_update(target, sl_side, live_qty, pending_stop, trade_id)
            if success:
                with PENDING_SL_LOCK:
                    if PENDING_SL.get(trade_id) == pending_stop:
                        PENDING_SL.pop(trade_id, None)
                break

        with PENDING_SL_LOCK:
            if trade_id not in PENDING_SL:
                break

    with PENDING_SL_LOCK:
        RETRY_THREAD_ACTIVE.discard(trade_id)

def process_trailing_signal(symbol: str, qty: float, sl_price: float, trade_id: int):
    target = clean_symbol(symbol)

    with ENGINE_LOCK:
        if BOT_STATE.get("trade_id") != trade_id:
            return

        live_qty, live_side, _ = get_exchange_position_state(target)
        if live_qty <= 0:
            purge_state()
            return

        new_stop = float(f"{sl_price:.2f}")
        if new_stop <= 0:
            return

        old_stop = float(BOT_STATE.get("sl_price") or 0.0)
        if live_side == "BUY":
            if old_stop > 0 and new_stop <= old_stop:
                return
            sl_side = "SELL"
        else:
            if old_stop > 0 and new_stop >= old_stop:
                return
            sl_side = "BUY"

        success = attempt_stop_update(target, sl_side, live_qty, new_stop, trade_id)
        if success:
            with PENDING_SL_LOCK:
                PENDING_SL.pop(trade_id, None)
            return

        with PENDING_SL_LOCK:
            PENDING_SL[trade_id] = new_stop
            if trade_id not in RETRY_THREAD_ACTIVE:
                RETRY_THREAD_ACTIVE.add(trade_id)
                threading.Thread(
                    target=sl_update_retry_loop,
                    args=(target, trade_id),
                    daemon=True
                ).start()

def process_cancel_entry(symbol: str, trade_id: int):
    with ENGINE_LOCK:
        if BOT_STATE.get("trade_id") != trade_id:
            return
        entry_id = BOT_STATE.get("entry_id")
        if entry_id:
            cancel_order(entry_id)
        BOT_STATE["entry_id"] = None
        BOT_STATE["entry_active"] = False
        save_state()

def process_close(symbol: str):
    target = clean_symbol(symbol)
    with ENGINE_LOCK:
        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])

        live_qty, live_side, _ = get_exchange_position_state(target)
        if live_qty > 0:
            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)
            if not wait_until_flat(target, timeout_sec=5):
                return

        if BOT_STATE.get("sl_id"):
            cancel_order(BOT_STATE["sl_id"])

        purge_state()

# =============================================================================
# FASTAPI LIFECYCLE & ROUTES
# =============================================================================

def clock_resync_loop():
    while True:
        time.sleep(300)
        sync_clock_directly()

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    sync_clock_directly()
    print(f"[BOOT] Clock Offset = {CLOCK_OFFSET_MS}ms", flush=True)
    threading.Thread(target=clock_resync_loop, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online"}

@app.get("/diag/outbound-ip")
async def diag_outbound_ip():
    try:
        r = requests.get("https://api.ipify.org?format=json", timeout=4)
        return {"outbound_ip": r.json().get("ip"), "status": r.status_code}
    except Exception as e:
        return {"error": str(e)}

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
    qty = float(data.get("quantity", 0.002))
    price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))
    timeout_sec = int(data.get("timeout_sec", 300))

    if action == "UPDATE_SL" and sl_price == 0.0 and price > 0.0:
        sl_price = price

    print(
        f"[WEBHOOK] Action={action} TradeID={trade_id} "
        f"Qty={qty} Price={price} SL={sl_price}",
        flush=True
    )

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

    elif action == "CANCEL_ENTRY":
        threading.Thread(
            target=process_cancel_entry,
            args=(symbol, trade_id),
            daemon=True
        ).start()

    elif action in ["SL_EXIT", "EXIT", "CLOSE"]:
        threading.Thread(
            target=process_close,
            args=(symbol,),
            daemon=True
        ).start()

    return {"status": "accepted"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
