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
    "exchange_order_id": None,
    "sl_id": None,

    "fill_price": 0.0,
    "sl_price": 0.0,

    "entry_active": False,
    "position_active": False
}

SL_WATCH_LOCK = threading.Lock()
SL_WATCH = {}


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
                f"[STATE LOADED] TradeID={BOT_STATE.get('trade_id')} "
                f"Side={BOT_STATE.get('side')} Qty={BOT_STATE.get('qty')}",
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
    BOT_STATE["exchange_order_id"] = None
    BOT_STATE["sl_id"] = None
    BOT_STATE["fill_price"] = 0.0
    BOT_STATE["sl_price"] = 0.0
    BOT_STATE["entry_active"] = False
    BOT_STATE["position_active"] = False
    save_state()
    remove_sl_watch(old_sl_id)


# =============================================================================
# CLOCK
# =============================================================================

def sync_clock_directly() -> int:
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
    global LAST_USED_TIMESTAMP
    with TS_LOCK:
        now_ts = int(time.time() * 1000) + CLOCK_OFFSET_MS
        if now_ts <= LAST_USED_TIMESTAMP:
            now_ts = LAST_USED_TIMESTAMP + 1
        LAST_USED_TIMESTAMP = now_ts
        return now_ts


# =============================================================================
# LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    sync_clock_directly()
    print(f"[BOOT] Clock Offset = {CLOCK_OFFSET_MS}ms", flush=True)
    yield


app = FastAPI(lifespan=lifespan)


# =============================================================================
# HELPERS
# =============================================================================

def clean_symbol(sym: str) -> str:
    if not sym:
        return DEFAULT_SYMBOL
    return (
        sym.replace("-", "")
        .replace("_", "")
        .replace(".P", "")
        .replace(".p", "")
        .replace("/", "")
        .upper()
    )


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


# =============================================================================
# SIGNED ORDER REQUEST
# =============================================================================

def send_signed_order(payload: dict, max_retries: int = 3):
    for attempt in range(max_retries):
        if attempt > 0:
            sync_clock_directly()
            time.sleep(0.15)

        payload["timestamp"] = get_synced_time()
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        headers = sign_data(body)

        try:
            r = requests.post(
                f"{SHARK_BASE_URL}/v1/order/place-order",
                data=body,
                headers=headers,
                timeout=4
            )

            if r.status_code in [200, 201]:
                res = r.json()
                data = res.get("data", {})

                # Extract both clientOrderId and the exchange numeric order id
                cid = (
                    res.get("clientOrderId")
                    or (data.get("clientOrderId") if isinstance(data, dict) else None)
                    or (data.get("orderId") if isinstance(data, dict) else None)
                    or res.get("orderId")
                )
                raw_id = res.get("id") or (data.get("id") if isinstance(data, dict) else None)

                print(f"[ORDER SUCCESS] {res}", flush=True)
                return True, cid, raw_id

            print(f"[EXCHANGE REJECTED] HTTP {r.status_code}: {r.text}", flush=True)

            if "4007" in r.text or "Signature mismatch" in r.text or "timestamp" in r.text.lower():
                print(f"[SIGNATURE RETRY] Attempt {attempt + 1}", flush=True)
                continue

            return False, None, None

        except Exception as e:
            print(f"[DISPATCH ERROR] {e}", flush=True)
            time.sleep(0.10)

    return False, None, None


# =============================================================================
# CANCEL ORDER
# =============================================================================

def cancel_order(client_order_id: str) -> bool:
    if not client_order_id:
        return True

    payload = {
        "clientOrderId": str(client_order_id),
        "timestamp": get_synced_time()
    }
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_data(body)

    try:
        r = requests.delete(
            f"{SHARK_BASE_URL}/v1/order/delete-order",
            data=body,
            headers=headers,
            timeout=4
        )

        if r.status_code in [200, 201, 204] or "3060" in r.text or "not found" in r.text.lower():
            print(f"[ORDER RESOLVED/CANCELLED] {client_order_id}", flush=True)
            return True

        print(f"[CANCEL FAILED] {r.status_code}: {r.text}", flush=True)
        return False

    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)
        return False


# =============================================================================
# EDIT STOP ORDER
# =============================================================================

def edit_stop_order(
    client_order_id: str,
    quantity: float,
    stop_price: float,
    limit_price: float
) -> bool:
    if not client_order_id:
        return False

    payload = {
        "clientOrderId": str(client_order_id),
        "quantity": float(f"{quantity:.4f}"),
        "stopPrice": float(f"{stop_price:.2f}"),
        "price": float(f"{limit_price:.2f}"),
        "timestamp": get_synced_time()
    }
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    headers = sign_data(body)

    try:
        r = requests.patch(
            f"{SHARK_BASE_URL}/v1/order/edit-order",
            data=body,
            headers=headers,
            timeout=4
        )

        if r.status_code in [200, 201]:
            print(
                f"[STOP EDITED] ID={client_order_id} "
                f"Stop={stop_price} Limit={limit_price}",
                flush=True
            )
            return True

        print(f"[STOP EDIT FAILED] {r.status_code}: {r.text}", flush=True)

    except Exception as e:
        print(f"[STOP EDIT ERROR] {e}", flush=True)

    return False


# =============================================================================
# EXCHANGE POSITION
# =============================================================================

def get_exchange_position_state(symbol: str):
    target = clean_symbol(symbol)
    ts = str(get_synced_time())

    # Check both filtered and unfiltered variants
    endpoints = [
        f"/v1/position/open-positions?symbol={target}&timestamp={ts}",
        f"/v1/positions?symbol={target}&timestamp={ts}",
        f"/v1/position/open-positions?timestamp={ts}",
        f"/v1/positions?timestamp={ts}"
    ]

    for ep in endpoints:
        try:
            querystr = ep.split("?")[1]
            headers = sign_data(querystr)

            r = requests.get(
                f"{SHARK_BASE_URL}{ep}",
                headers=headers,
                timeout=3
            )

            if r.status_code != 200:
                continue

            res = r.json()
            data = res.get("data", res)

            if isinstance(data, dict):
                items = data.get("list") or data.get("positions") or [data]
            elif isinstance(data, list):
                items = data
            else:
                items = [data]

            for pos in items:
                if not isinstance(pos, dict):
                    continue

                pos_sym = clean_symbol(str(pos.get("symbol", "")))
                if pos_sym != target and target not in pos_sym:
                    continue

                raw_qty = float(
                    pos.get("positionAmt")
                    or pos.get("size")
                    or pos.get("positionAmount")
                    or pos.get("total")
                    or 0.0
                )

                side_str = str(pos.get("side") or pos.get("positionSide") or "").upper()

                if abs(raw_qty) > 0.000001:
                    resolved_side = "SELL" if (side_str in ["SHORT", "SELL"] or raw_qty < 0) else "BUY"
                    return abs(raw_qty), resolved_side

        except Exception as e:
            print(f"[POS CHECK ERROR] {e}", flush=True)
            continue

    return 0.0, "FLAT"


def wait_until_flat(symbol: str, timeout_sec: float = 5.0) -> bool:
    start = time.time()
    while time.time() - start < timeout_sec:
        qty, _ = get_exchange_position_state(symbol)
        if qty <= 0.000001:
            return True
        time.sleep(0.20)
    qty, _ = get_exchange_position_state(symbol)
    return qty <= 0.000001


# =============================================================================
# TICKER
# =============================================================================

def get_current_ticker_price(symbol: str) -> float:
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"symbol={target}&timestamp={ts}"
    headers = sign_data(query)

    try:
        r = requests.get(
            f"{SHARK_BASE_URL}/v1/ticker/price?{query}",
            headers=headers,
            timeout=3
        )

        if r.status_code == 200:
            res = r.json()
            data = res.get("data", res)
            if isinstance(data, dict):
                return float(data.get("price") or data.get("lastPrice") or 0.0)
    except Exception:
        pass

    return 0.0


# =============================================================================
# STOP LOSS
# =============================================================================

def place_stop_loss(
    symbol: str,
    side: str,
    qty: float,
    stop_price: float,
    trade_id: int
):
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
        "type": "STOP_LIMIT",
        "userCategory": "EXTERNAL"
    }

    print(f"[STOP PLACE] {side} Stop={stop_price} Limit={limit_price}", flush=True)

    ok, cid, _ = send_signed_order(payload)

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


# =============================================================================
# STOP MONITOR
# =============================================================================

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

        live_qty, _ = get_exchange_position_state(target)

        if live_qty <= 0.000001:
            with ENGINE_LOCK:
                if BOT_STATE.get("trade_id") == trade_id:
                    print("[STOP MONITOR] Position is FLAT. Clearing trade state.", flush=True)
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
                    cancel_order(sl_id)
                    flatten_position(target, sl_side, live_qty)
                    wait_until_flat(target, timeout_sec=5)
                    purge_state()

            remove_sl_watch(sl_id)
            return

        if time.time() - start > 86400:
            print("[STOP MONITOR] 24h watchdog restart.", flush=True)
            start = time.time()


# =============================================================================
# MARKET FLATTEN
# =============================================================================

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
        "type": "MARKET",
        "userCategory": "EXTERNAL"
    }

    print(f"[MARKET EXIT] {side} {qty} {target}", flush=True)
    ok, _, _ = send_signed_order(payload)
    return ok


# =============================================================================
# ORDER STATUS
# =============================================================================

def check_order_status(client_order_id: str, symbol: str, exchange_id: int = None):
    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    
    # Try querying by exchange id first if known, otherwise by clientOrderId
    queries = []
    if exchange_id:
        queries.append(f"orderId={exchange_id}&symbol={target}&timestamp={ts}")
    if client_order_id:
        queries.append(f"clientOrderId={client_order_id}&symbol={target}&timestamp={ts}")

    for query in queries:
        try:
            headers = sign_data(query)
            r = requests.get(
                f"{SHARK_BASE_URL}/v1/order/order-detail?{query}",
                headers=headers,
                timeout=3
            )

            if r.status_code != 200:
                continue

            res = r.json()
            print(f"[ORDER STATUS RAW] {res}", flush=True)

            order = res.get("data", res)
            if isinstance(order, dict):
                if "order" in order and isinstance(order["order"], dict):
                    order = order["order"]

                status = str(
                    order.get("status")
                    or order.get("orderStatus")
                    or order.get("state")
                    or ""
                ).upper()

                order_amount = float(
                    order.get("orderAmount")
                    or order.get("quantity")
                    or order.get("origQty")
                    or 0.0
                )

                filled_amount = float(
                    order.get("filledAmount")
                    or order.get("executedQty")
                    or order.get("cumQty")
                    or order.get("filledQty")
                    or order.get("filledQuantity")
                    or 0.0
                )

                avg_price = float(
                    order.get("avgPrice")
                    or order.get("avgFillPrice")
                    or order.get("averagePrice")
                    or order.get("executedPrice")
                    or order.get("price")
                    or order.get("limitPrice")
                    or 0.0
                )

                if status in ["CANCELED", "CANCELLED", "REJECTED", "EXPIRED"]:
                    return "CANCELLED", -1.0

                if status in ["FILLED", "SUCCESS", "EXECUTED", "COMPLETE"]:
                    return "FILLED", avg_price

                if order_amount > 0 and filled_amount >= order_amount * 0.999:
                    return "FILLED", avg_price

                if filled_amount > 0:
                    return "PARTIAL", avg_price

                return "OPEN", 0.0

        except Exception as e:
            print(f"[ORDER STATUS ERROR] {e}", flush=True)

    return "UNKNOWN", 0.0


# =============================================================================
# ENTRY WATCHER
# =============================================================================

def entry_order_watcher(
    symbol: str,
    side: str,
    limit_price: float,
    trade_id: int,
    qty: float,
    order_id: str,
    timeout_sec: int,
    exchange_id: int = None
):
    target = clean_symbol(symbol)
    start = time.time()

    print(f"[ENTRY WATCHER] ID={order_id} ExchangeID={exchange_id} Timeout={timeout_sec}s", flush=True)

    while time.time() - start < timeout_sec:
        time.sleep(1.0)

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id or BOT_STATE.get("entry_id") != order_id:
                return

        status, exec_price = check_order_status(order_id, target, exchange_id)

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
            backstop_qty, backstop_side = get_exchange_position_state(target)
            if backstop_qty >= qty * 0.999 and backstop_side == side:
                filled_via_backstop = True
                print(
                    f"[ENTRY WATCHER] Detected live position via backstop: "
                    f"{backstop_side} Qty={backstop_qty}. Triggering stop placement.",
                    flush=True
                )

        if filled_via_order_detail or filled_via_backstop:
            live_qty, live_side = get_exchange_position_state(target)
            if live_qty <= 0:
                live_qty = qty
                live_side = side

            if exec_price > 0:
                real_fill = exec_price
            else:
                ticker_price = get_current_ticker_price(target)
                real_fill = ticker_price if ticker_price > 0 else limit_price

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
                else:
                    print("[CRITICAL] Initial SL FAILED. Attempting MARKET EXIT.", flush=True)
                    flatten_position(target, sl_side, live_qty)
                    wait_until_flat(target, 5)
                    purge_state()
                    return

                save_state()

            return

    print(f"[ENTRY TIMEOUT] Cancelling {order_id}", flush=True)

    with ENGINE_LOCK:
        cancel_order(order_id)
        if BOT_STATE.get("entry_id") == order_id:
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False
            save_state()


# =============================================================================
# ENTRY SIGNAL HANDLER
# =============================================================================

def process_entry_signal(
    action: str,
    symbol: str,
    qty: float,
    price: float,
    trade_id: int,
    timeout_sec: int
):
    target = clean_symbol(symbol)
    side = "BUY" if "BUY" in action else "SELL"

    with ENGINE_LOCK:
        current_id = BOT_STATE.get("trade_id")
        if current_id is not None and trade_id < current_id:
            print(f"[OLD SIGNAL IGNORED] {trade_id} < {current_id}", flush=True)
            return

        live_qty, live_side = get_exchange_position_state(target)

        # Handle Reversal
        if live_qty > 0 and live_side != side:
            print(f"[REVERSAL] Closing {live_side} before entering {side}", flush=True)
            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)

            confirmed_flat = wait_until_flat(target, timeout_sec=5)
            if not confirmed_flat:
                print("[REVERSAL ABORTED] Old position did not confirm flat.", flush=True)
                return

            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])

            purge_state()

        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False

        live_qty, live_side = get_exchange_position_state(target)
        if live_qty > 0:
            print(f"[ENTRY BLOCKED] Existing {live_side} position Qty={live_qty}", flush=True)
            return

        BOT_STATE["trade_id"] = trade_id
        BOT_STATE["side"] = side
        BOT_STATE["qty"] = qty
        BOT_STATE["entry_id"] = None
        BOT_STATE["exchange_order_id"] = None
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
            "type": "LIMIT",
            "userCategory": "EXTERNAL"
        }

        print(f"[ENTRY] {side} {clean_qty} {target} @ {clean_price}", flush=True)

        ok, cid, raw_id = send_signed_order(payload)

        if not ok or not cid:
            print("[ENTRY FAILED]", flush=True)
            purge_state()
            return

        BOT_STATE["entry_id"] = cid
        BOT_STATE["exchange_order_id"] = raw_id
        save_state()

        threading.Thread(
            target=entry_order_watcher,
            args=(target, side, clean_price, trade_id, clean_qty, cid, timeout_sec, raw_id),
            daemon=True
        ).start()


# =============================================================================
# TRAILING STOP SIGNAL
# =============================================================================

def process_trailing_signal(symbol: str, qty: float, sl_price: float, trade_id: int):
    target = clean_symbol(symbol)

    with ENGINE_LOCK:
        if BOT_STATE.get("trade_id") != trade_id:
            print("[UPDATE_SL IGNORED] Trade ID mismatch.", flush=True)
            return

        live_qty, live_side = get_exchange_position_state(target)

        if live_qty <= 0:
            print("[UPDATE_SL] Exchange position is flat.", flush=True)
            purge_state()
            return

        new_stop = float(f"{sl_price:.2f}")
        if new_stop <= 0:
            return

        old_stop = float(BOT_STATE.get("sl_price") or 0.0)

        if live_side == "BUY":
            if old_stop > 0 and new_stop <= old_stop:
                print("[UPDATE_SL IGNORED] BUY stop would move backwards.", flush=True)
                return
            sl_side = "SELL"
        else:
            if old_stop > 0 and new_stop >= old_stop:
                print("[UPDATE_SL IGNORED] SELL stop would move backwards.", flush=True)
                return
            sl_side = "BUY"

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
                return

            print("[STOP EDIT FAILED] Using cancel/recreate fallback.", flush=True)
            cancel_order(old_sl_id)
            remove_sl_watch(old_sl_id)
            BOT_STATE["sl_id"] = None

        new_sl_id = place_stop_loss(target, sl_side, live_qty, new_stop, trade_id)

        if new_sl_id:
            BOT_STATE["sl_id"] = new_sl_id
            BOT_STATE["sl_price"] = new_stop
            save_state()
            print(f"[STOP UPDATED] {new_stop}", flush=True)
        else:
            print("[CRITICAL] Unable to recreate SL.", flush=True)
            flatten_position(target, sl_side, live_qty)
            wait_until_flat(target, 5)
            purge_state()


# =============================================================================
# CANCEL PENDING ENTRY
# =============================================================================

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


# =============================================================================
# CLOSE EVERYTHING
# =============================================================================

def process_close(symbol: str):
    target = clean_symbol(symbol)
    with ENGINE_LOCK:
        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])

        live_qty, live_side = get_exchange_position_state(target)
        if live_qty > 0:
            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)
            confirmed_flat = wait_until_flat(target, timeout_sec=5)
            if not confirmed_flat:
                print("[CLOSE ABORTED] Position did not confirm flat.", flush=True)
                return

        if BOT_STATE.get("sl_id"):
            cancel_order(BOT_STATE["sl_id"])

        purge_state()


# =============================================================================
# WEBHOOK ENDPOINTS
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


# =============================================================================
# START SERVER
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("shark_bridge:app", host="0.0.0.0", port=port)
