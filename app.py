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

# Distance (in points) between the actual exchange fill price and the
# initial protective stop. Anchored to the REAL fill price reported by
# Shark, never to TradingView's price - this is intentional per your spec.
INITIAL_SL_POINTS = 100.0

# Buffer between a stop's trigger price and its limit price for STOP_LIMIT
# orders placed on Shark.
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

# -----------------------------------------------------------------------------
# FIX 2: SL_WATCH holds the *live* stop/limit price each monitor thread should
# be checking against. Previously monitor_stop_order() captured stop_price /
# limit_price as plain function arguments at thread-start time, so a
# successful in-place edit_stop_order() call (which keeps the same sl_id)
# left the running monitor thread comparing live price against the OLD
# buffer forever. Now the monitor thread re-reads this dict every loop
# iteration, and any code path that changes a stop's price also updates the
# dict, so the monitor is always working off the current exchange state.
# -----------------------------------------------------------------------------
SL_WATCH_LOCK = threading.Lock()
SL_WATCH = {}

# Tracks which order IDs have already had their check_order_status() 404
# logged once, so a permanently-broken endpoint doesn't spam identical
# lines on every 0.8s poll for the whole entry-timeout window.
ORDER_STATUS_FAIL_LOGGED = set()

# Throttled raw logging for get_exchange_position_state(): a *successful*
# (HTTP 200) response that simply doesn't contain the position we expect
# was previously invisible - only outright errors got logged. This prints
# the raw response periodically (not every 0.8s poll) so an incident like
# "exchange has an open position but our check reported FLAT" can actually
# be diagnosed after the fact instead of leaving zero trace.
POSITION_RAW_LOG_LOCK = threading.Lock()
LAST_POSITION_RAW_LOG = 0.0
POSITION_RAW_LOG_INTERVAL_SEC = 8.0


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
# CLOCK
# =============================================================================

def sync_clock_directly() -> int:
    global CLOCK_OFFSET_MS

    try:
        r = requests.get(
            f"{SHARK_BASE_URL}/v1/time",
            timeout=2
        )

        if r.status_code == 200:

            res = r.json()

            srv_ts = int(
                res.get("serverTime")
                or res.get("data")
                or 0
            )

            if srv_ts > 0:
                CLOCK_OFFSET_MS = (
                    srv_ts - int(time.time() * 1000)
                )

                return srv_ts

    except Exception:
        pass

    return int(time.time() * 1000) + CLOCK_OFFSET_MS


def get_synced_time() -> int:
    global LAST_USED_TIMESTAMP

    with TS_LOCK:

        now_ts = (
            int(time.time() * 1000)
            + CLOCK_OFFSET_MS
        )

        if now_ts <= LAST_USED_TIMESTAMP:
            now_ts = LAST_USED_TIMESTAMP + 1

        LAST_USED_TIMESTAMP = now_ts

        return now_ts


# =============================================================================
# FASTAPI
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):

    load_state()
    sync_clock_directly()

    print(
        f"[BOOT] Clock Offset = {CLOCK_OFFSET_MS}ms",
        flush=True
    )

    # Containers can take a few seconds after boot for their clock to
    # settle, and a single one-time sync at startup never gets refreshed
    # again unless an order actually fails first. Keep resyncing in the
    # background for the life of the process so drift is caught proactively
    # instead of reactively (after a real order has already been rejected).
    threading.Thread(target=clock_resync_loop, daemon=True).start()

    yield


def clock_resync_loop():
    # Extra-frequent resyncs right after boot, when drift is most likely,
    # then settle into a steady cadence for the rest of the process life.
    for _ in range(6):
        time.sleep(5)
        sync_clock_directly()
        print(f"[CLOCK RESYNC] Offset = {CLOCK_OFFSET_MS}ms", flush=True)

    while True:
        time.sleep(30)
        sync_clock_directly()
        print(f"[CLOCK RESYNC] Offset = {CLOCK_OFFSET_MS}ms", flush=True)


app = FastAPI(lifespan=lifespan)


# =============================================================================
# HELPERS
# =============================================================================

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
            time.sleep(0.10)

        payload["timestamp"] = get_synced_time()

        body = json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True
        )

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

                if isinstance(data, dict):
                    cid = (
                        res.get("clientOrderId")
                        or data.get("clientOrderId")
                        or data.get("orderId")
                    )
                else:
                    cid = (
                        res.get("clientOrderId")
                        or res.get("orderId")
                    )

                print(f"[ORDER SUCCESS] {res}", flush=True)

                return True, cid

            if (
                r.status_code == 403
                or "4007" in r.text
                or "Signature mismatch" in r.text
            ):
                print(f"[SIGNATURE RETRY] Attempt {attempt + 1}", flush=True)
                continue

            print(
                f"[EXCHANGE REJECTED] HTTP {r.status_code}: {r.text}",
                flush=True
            )

            return False, None

        except Exception as e:
            print(f"[DISPATCH ERROR] {e}", flush=True)
            time.sleep(0.10)

    return False, None


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

        if r.status_code in [200, 201, 204]:
            print(f"[ORDER CANCELLED] {client_order_id}", flush=True)
            return True

        print(f"[CANCEL FAILED] {r.status_code}: {r.text}", flush=True)
        return False

    except Exception as e:
        print(f"[CANCEL ERROR] {e}", flush=True)
        return False


# =============================================================================
# EDIT EXISTING STOP ORDER
# Shark supports PATCH /v1/order/edit-order
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
    """
    Uses the documented endpoint: GET /v1/positions/{positionStatus}
    (there is no /v1/position/open-positions or bare /v1/positions in
    Shark's actual API - those were guessed and never worked).
    Response fields per docs: contractPair (not symbol), positionType
    ("LONG"/"SHORT", not side/positionSide), positionAmount (already
    unsigned - direction comes from positionType, not a negative sign),
    entryPrice (the exchange's own recorded average entry price - this
    is what the position UI's entry line actually shows, and should be
    preferred over a ticker snapshot when calculating the initial SL).

    Returns (qty, side, entry_price). Callers that don't need entry
    price can unpack with `qty, side, _ = ...`.
    """

    target = clean_symbol(symbol)
    ts = str(get_synced_time())

    params = {
        "sortOrder": "desc",
        "pageSize": "100",
        "symbol": target,
        "timestamp": ts
    }
    query_string = "&".join(f"{k}={v}" for k, v in params.items())

    endpoint = f"/v1/positions/OPEN?{query_string}"
    headers = sign_data(query_string)

    try:
        r = requests.get(
            f"{SHARK_BASE_URL}{endpoint}",
            headers=headers,
            timeout=3
        )

        if r.status_code != 200:
            print(
                f"[POSITION CHECK FAILED] {endpoint} -> "
                f"HTTP {r.status_code}: {r.text}",
                flush=True
            )
            return 0.0, "FLAT", 0.0

        res = r.json()

        # Throttled visibility into what a HEALTHY (200) response actually
        # contains - this is what was missing when a real open position
        # went undetected without any error being logged anywhere.
        global LAST_POSITION_RAW_LOG
        now = time.time()
        with POSITION_RAW_LOG_LOCK:
            should_log = (now - LAST_POSITION_RAW_LOG) >= POSITION_RAW_LOG_INTERVAL_SEC
            if should_log:
                LAST_POSITION_RAW_LOG = now
        if should_log:
            print(f"[POSITION RAW] target={target} -> {res}", flush=True)

        data = res if isinstance(res, list) else res.get("data", res)
        items = data if isinstance(data, list) else [data]

        for pos in items:
            if not isinstance(pos, dict):
                continue

            sym = str(
                pos.get("contractPair") or pos.get("symbol") or ""
            ).upper()
            if target not in sym and sym not in target:
                continue

            raw_qty = float(
                pos.get("positionAmount")
                or pos.get("quantity")
                or pos.get("positionAmt")
                or pos.get("size")
                or 0.0
            )

            position_type = str(
                pos.get("positionType")
                or pos.get("side")
                or pos.get("positionSide")
                or ""
            ).upper()

            entry_price = float(pos.get("entryPrice") or 0.0)

            if abs(raw_qty) > 0.000001:
                if position_type in ["SHORT", "SELL"] or raw_qty < 0:
                    return abs(raw_qty), "SELL", entry_price
                return abs(raw_qty), "BUY", entry_price

        return 0.0, "FLAT", 0.0

    except Exception as e:
        print(f"[POSITION CHECK ERROR] {endpoint} -> {e}", flush=True)
        return 0.0, "FLAT", 0.0


# =============================================================================
# WAIT UNTIL EXCHANGE POSITION IS FLAT
# =============================================================================

def wait_until_flat(symbol: str, timeout_sec: float = 5.0) -> bool:

    start = time.time()

    while time.time() - start < timeout_sec:
        qty, _, _ = get_exchange_position_state(symbol)
        if qty <= 0.000001:
            return True
        time.sleep(0.20)

    qty, _, _ = get_exchange_position_state(symbol)
    return qty <= 0.000001


# =============================================================================
# TICKER
# =============================================================================

def get_current_ticker_price(symbol: str) -> float:
    """
    Uses the documented PUBLIC endpoint: GET /v1/market/ticker24Hr/{pair}
    (there is no /v1/ticker/price in Shark's actual API - that was
    guessed and never worked). No signature/timestamp required - this
    is a public market data endpoint. Response field "c" = last price.
    """

    pair = clean_symbol(symbol).lower()

    try:
        r = requests.get(
            f"{SHARK_BASE_URL}/v1/market/ticker24Hr/{pair}",
            timeout=3
        )

        if r.status_code == 200:
            res = r.json()
            data = res.get("data", res)
            if isinstance(data, dict):
                price = (
                    data.get("c")
                    or data.get("price")
                    or data.get("lastPrice")
                )
                if price is not None:
                    return float(price)
        else:
            print(
                f"[TICKER CHECK FAILED] {pair} -> "
                f"HTTP {r.status_code}: {r.text}",
                flush=True
            )

    except Exception as e:
        print(f"[TICKER CHECK ERROR] {pair} -> {e}", flush=True)

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
    """
    side  = the SL order's own side (opposite of the position side)
    qty   = position quantity being protected
    """

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

    print(
        f"[STOP PLACE] {side} Stop={stop_price} Limit={limit_price}",
        flush=True
    )

    ok, cid = send_signed_order(payload)

    if ok and cid:
        print(f"[STOP PLACED] ID={cid}", flush=True)

        # FIX 2: register the live price this monitor thread should track,
        # instead of baking it into the thread's arguments.
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
# FIX 2: no longer takes stop_price/limit_price/side/qty as fixed arguments.
# Instead it re-reads SL_WATCH[sl_id] every loop iteration, so any code path
# that edits the stop in place (edit_stop_order) can update the buffer this
# thread checks against just by calling update_sl_watch().
# =============================================================================

def monitor_stop_order(symbol: str, sl_id: str, trade_id: int):

    target = clean_symbol(symbol)
    start = time.time()

    while True:
        time.sleep(0.8)

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id:
                remove_sl_watch(sl_id)
                return
            if BOT_STATE.get("sl_id") != sl_id:
                remove_sl_watch(sl_id)
                return

        watch = get_sl_watch(sl_id)
        if watch is None:
            # Stop was cancelled/replaced elsewhere; nothing left to watch.
            return

        limit_price = watch["limit"]
        sl_side = watch["side"]

        live_qty, _, _ = get_exchange_position_state(target)

        # Position is gone = SL (or something else) already executed.
        if live_qty <= 0.000001:
            with ENGINE_LOCK:
                if BOT_STATE.get("trade_id") == trade_id:
                    print(
                        "[STOP MONITOR] Position is FLAT. Clearing trade state.",
                        flush=True
                    )
                    purge_state()
            remove_sl_watch(sl_id)
            return

        curr_price = get_current_ticker_price(target)
        if curr_price <= 0:
            continue

        # Emergency protection for STOP_LIMIT gap - always checked against
        # the CURRENT buffer, even if it was just moved by a trailing update.
        if sl_side == "SELL":
            breached = curr_price < limit_price
        else:
            breached = curr_price > limit_price

        if breached:
            print(
                f"[STOP GAP] Price={curr_price} Limit={limit_price}",
                flush=True
            )

            with ENGINE_LOCK:
                if (
                    BOT_STATE.get("sl_id") == sl_id
                    and BOT_STATE.get("trade_id") == trade_id
                ):
                    # Flatten FIRST, before touching the existing stop. If
                    # the market exit fails, the resting STOP_LIMIT is the
                    # only protection this position has - cancelling it
                    # first (as this used to do) would leave the position
                    # completely naked on a failed flatten.
                    flatten_position(target, sl_side, live_qty)
                    confirmed_flat = wait_until_flat(target, timeout_sec=5)

                    if confirmed_flat:
                        cancel_order(sl_id)
                        purge_state()
                    else:
                        print(
                            "[CRITICAL] Emergency flatten FAILED during "
                            f"STOP GAP handling. Position (side={sl_side}, "
                            f"qty={live_qty}) may still be open. Existing "
                            "stop order left in place as a last line of "
                            "defense; state NOT purged - MANUAL CHECK "
                            "REQUIRED.",
                            flush=True
                        )
                        remove_sl_watch(sl_id)
                        return

            remove_sl_watch(sl_id)
            return

        # Safety watchdog - just resets the loop timer, doesn't exit.
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

    ok, _ = send_signed_order(payload)
    return ok


# =============================================================================
# ORDER STATUS
# =============================================================================

def check_order_status(client_order_id: str, symbol: str):
    """
    Uses the documented endpoint: GET /v1/order/{clientOrderId}
    (there is no /v1/order/order-detail in Shark's actual API - that
    was guessed and never worked, which is why fills were never being
    detected). Response fields per docs: status, filledQty (net filled),
    executedQty (last filled), avgPrice, quantity (order size).

    NOTE: as of the last confirmed test, this documented endpoint itself
    returns a framework-level 404 ("Cannot GET ...") on Shark's production
    API, suggesting it isn't actually live there despite the docs. Fill
    detection therefore currently relies on entry_order_watcher's
    live-position backstop, which IS confirmed working. This function is
    left in place in case Shark fixes/relocates the endpoint - only the
    first failure per order is logged to avoid spamming identical 404s
    every 0.8s for the whole polling window.
    """

    target = clean_symbol(symbol)
    ts = str(get_synced_time())
    query = f"timestamp={ts}"
    headers = sign_data(query)

    try:
        r = requests.get(
            f"{SHARK_BASE_URL}/v1/order/{client_order_id}?{query}",
            headers=headers,
            timeout=3
        )

        if r.status_code != 200:
            if client_order_id not in ORDER_STATUS_FAIL_LOGGED:
                print(
                    f"[ORDER STATUS FAILED] ID={client_order_id} -> "
                    f"HTTP {r.status_code}: {r.text} "
                    f"(further identical failures for this order suppressed)",
                    flush=True
                )
                ORDER_STATUS_FAIL_LOGGED.add(client_order_id)
            return "UNKNOWN", 0.0

        res = r.json()

        # TEMP DEBUG - safe to remove once this is confirmed working
        # against the corrected /v1/order/{clientOrderId} endpoint.
        print(f"[ORDER STATUS RAW] {res}", flush=True)

        order = res.get("data", res) if isinstance(res, dict) else res

        if isinstance(order, dict):

            status = str(order.get("status") or "").upper()

            order_amount = float(
                order.get("quantity")
                or order.get("orderAmount")
                or order.get("origQty")
                or 0.0
            )

            filled_amount = float(
                order.get("filledQty")
                or order.get("executedQty")
                or order.get("filledAmount")
                or 0.0
            )

            avg_price = float(
                order.get("avgPrice")
                or order.get("price")
                or order.get("limitPrice")
                or 0.0
            )

            if status in ["CANCELED", "CANCELLED", "REJECTED", "EXPIRED"]:
                return "CANCELLED", -1.0

            if status in ["FILLED", "SUCCESS", "EXECUTED", "COMPLETE"]:
                return "FILLED", avg_price

            # No usable status string, but the amounts tell the real story.
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
# Waits up to timeout_sec for the limit entry to fill. Only once a fill is
# confirmed does it read the REAL fill price from Shark and place the SL
# INITIAL_SL_POINTS away from it. If it times out unfilled, the order is
# simply cancelled - no SL is ever placed for an order that never filled.
# =============================================================================

def entry_order_watcher(
    symbol: str,
    side: str,
    limit_price: float,
    trade_id: int,
    qty: float,
    order_id: str,
    timeout_sec: int
):

    target = clean_symbol(symbol)
    start = time.time()
    last_heartbeat = start

    print(f"[ENTRY WATCHER] ID={order_id} Timeout={timeout_sec}s", flush=True)

    while time.time() - start < timeout_sec:

        time.sleep(0.8)

        if time.time() - last_heartbeat >= 10:
            print(
                f"[ENTRY WATCHER] Still waiting on {order_id} "
                f"({int(time.time() - start)}s elapsed)",
                flush=True
            )
            last_heartbeat = time.time()

        with ENGINE_LOCK:
            if BOT_STATE.get("trade_id") != trade_id:
                return
            if BOT_STATE.get("entry_id") != order_id:
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

        # -------------------------------------------------------------------
        # BACKSTOP: check the live exchange position independently of
        # check_order_status()'s parsing. If order-detail's field names are
        # ever wrong again (as happened - Shark uses orderAmount/filledAmount,
        # not executedQty/status), this still catches a real fill instead of
        # silently timing out and cancelling an already-filled order.
        # -------------------------------------------------------------------
        filled_via_backstop = False

        if not filled_via_order_detail:
            backstop_qty, backstop_side, _ = get_exchange_position_state(target)
            if backstop_qty >= qty * 0.999 and backstop_side == side:
                filled_via_backstop = True
                print(
                    f"[ENTRY WATCHER] Order-detail did not confirm, but "
                    f"exchange shows a live {backstop_side} position "
                    f"Qty={backstop_qty} - treating as FILLED via backstop.",
                    flush=True
                )

        if filled_via_order_detail or filled_via_backstop:

            live_qty, live_side, live_entry_price = get_exchange_position_state(target)
            if live_qty <= 0:
                continue

            # Real fill price, in priority order:
            # 1. exec_price from order-detail, when that endpoint works
            # 2. the exchange's own recorded position entryPrice - this is
            #    what the position UI's entry line shows, and is exact
            #    (not an approximation like the ticker snapshot below)
            # 3. current ticker price, as a rough fallback
            # 4. the originally requested limit price, as a last resort
            if exec_price > 0:
                real_fill = exec_price
                fill_source = "order-detail"
            elif live_entry_price > 0:
                real_fill = live_entry_price
                fill_source = "position entryPrice"
            else:
                ticker_price = get_current_ticker_price(target)
                real_fill = ticker_price if ticker_price > 0 else limit_price
                fill_source = "ticker" if ticker_price > 0 else "requested price (fallback)"

            print(
                f"[ENTRY FILLED] {live_side} Qty={live_qty} "
                f"Price={real_fill} (source: {fill_source})",
                flush=True
            )

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
                    print(
                        "[CRITICAL] Initial SL FAILED. Attempting MARKET EXIT.",
                        flush=True
                    )
                    flatten_position(target, sl_side, live_qty)
                    confirmed_flat = wait_until_flat(target, 5)

                    if confirmed_flat:
                        purge_state()
                    else:
                        print(
                            "[CRITICAL] Emergency flatten FAILED after initial "
                            f"SL failure. Position (side={live_side}, "
                            f"qty={live_qty}) is still open with NO stop. "
                            "State kept (not purged) - MANUAL INTERVENTION "
                            "REQUIRED.",
                            flush=True
                        )
                    return

                save_state()

            return

    # -------------------------------------------------------------------
    # TIMEOUT: cancel the unfilled order, no SL is ever placed for it.
    # -------------------------------------------------------------------
    print(f"[ENTRY TIMEOUT] Cancelling {order_id}", flush=True)

    with ENGINE_LOCK:
        cancel_order(order_id)
        if BOT_STATE.get("entry_id") == order_id:
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False
            save_state()


# =============================================================================
# ENTRY PROCESSOR
# FIX 1: in the reversal branch, the old stop is now only cancelled AFTER the
# flatten is confirmed. If the flatten can't be confirmed within the wait
# window, the function aborts WITHOUT touching the old stop - so the
# original position is never left naked.
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

        # ---------------------------------------------------------------
        # Ignore stale/out-of-order alerts
        # ---------------------------------------------------------------
        current_id = BOT_STATE.get("trade_id")
        if current_id is not None and trade_id < current_id:
            print(f"[OLD SIGNAL IGNORED] {trade_id} < {current_id}", flush=True)
            return

        # ---------------------------------------------------------------
        # Reverse an existing position, if the exchange actually has one
        # ---------------------------------------------------------------
        live_qty, live_side, _ = get_exchange_position_state(target)

        if live_qty > 0 and live_side != side:

            print(f"[REVERSAL] Closing {live_side} before {side}", flush=True)

            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)

            confirmed_flat = wait_until_flat(target, timeout_sec=5)

            if not confirmed_flat:
                # FIX 1: do NOT touch the old sl_id here. The original
                # position may still be open, so its protective stop must
                # be left exactly as-is rather than cancelled.
                print(
                    "[REVERSAL ABORTED] Old position did not confirm flat. "
                    "Existing stop left in place for safety.",
                    flush=True
                )
                return

            # Flatten confirmed - now it's safe to clean up the old stop.
            if BOT_STATE.get("sl_id"):
                cancel_order(BOT_STATE["sl_id"])

            purge_state()

        # ---------------------------------------------------------------
        # Cancel any old pending (unfilled) entry
        # ---------------------------------------------------------------
        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])
            BOT_STATE["entry_id"] = None
            BOT_STATE["entry_active"] = False

        # ---------------------------------------------------------------
        # If the exchange still shows a position, refuse to double-enter
        # ---------------------------------------------------------------
        live_qty, live_side, _ = get_exchange_position_state(target)

        if live_qty > 0:
            print(
                f"[ENTRY BLOCKED] Existing {live_side} position Qty={live_qty}",
                flush=True
            )
            return

        # ---------------------------------------------------------------
        # New trade state
        # ---------------------------------------------------------------
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

        # ---------------------------------------------------------------
        # Place LIMIT entry
        # ---------------------------------------------------------------
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
# TRAILING SL
# FIX 2: on a successful in-place edit, update_sl_watch() pushes the new
# stop/limit into the shared dict so the already-running monitor thread
# picks it up on its very next loop iteration - no stale price window.
# =============================================================================

def process_trailing_signal(symbol: str, qty: float, sl_price: float, trade_id: int):

    target = clean_symbol(symbol)

    with ENGINE_LOCK:

        if BOT_STATE.get("trade_id") != trade_id:
            print("[UPDATE_SL IGNORED] Trade ID mismatch.", flush=True)
            return

        live_qty, live_side, _ = get_exchange_position_state(target)

        if live_qty <= 0:
            print("[UPDATE_SL] Exchange position is flat.", flush=True)
            purge_state()
            return

        new_stop = float(f"{sl_price:.2f}")
        if new_stop <= 0:
            return

        old_stop = float(BOT_STATE.get("sl_price") or 0.0)

        # Never loosen the stop
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

        # -----------------------------------------------------------------
        # Try editing the existing Shark stop first
        # -----------------------------------------------------------------
        if old_sl_id:

            offset = STOP_LIMIT_BUFFER
            new_limit = round(new_stop - offset, 2) if sl_side == "SELL" else round(new_stop + offset, 2)

            if edit_stop_order(old_sl_id, live_qty, new_stop, new_limit):

                # FIX 2: push the new price into SL_WATCH so the existing
                # monitor thread for old_sl_id starts checking against it
                # immediately - it does not need a new thread.
                update_sl_watch(old_sl_id, stop_price=new_stop, limit_price=new_limit, qty=live_qty)

                BOT_STATE["qty"] = live_qty
                BOT_STATE["sl_price"] = new_stop
                save_state()

                print(f"[STOP UPDATED] {new_stop}", flush=True)
                return

            # Edit failed - fall back to cancel/recreate.
            print("[STOP EDIT FAILED] Using cancel/recreate fallback.", flush=True)
            cancel_order(old_sl_id)
            remove_sl_watch(old_sl_id)
            BOT_STATE["sl_id"] = None

        # ---------------------------------------------------------------
        # Recreate stop (also the path used when there was no old_sl_id)
        # ---------------------------------------------------------------
        new_sl_id = place_stop_loss(target, sl_side, live_qty, new_stop, trade_id)

        if new_sl_id:
            BOT_STATE["sl_id"] = new_sl_id
            BOT_STATE["sl_price"] = new_stop
            save_state()
            print(f"[STOP UPDATED] {new_stop}", flush=True)
        else:
            print("[CRITICAL] Unable to recreate SL. Attempting emergency flatten.", flush=True)
            flatten_position(target, sl_side, live_qty)
            confirmed_flat = wait_until_flat(target, 5)

            if confirmed_flat:
                purge_state()
            else:
                # The flatten failed too (as happened here - exchange
                # rejected both attempts). The position is confirmed still
                # open, so state must NOT be purged - doing so would make
                # the bot forget about a live, unprotected position
                # entirely, with nothing left watching it.
                print(
                    "[CRITICAL] Emergency flatten FAILED. Position is still "
                    f"open on the exchange (side={sl_side and ('SELL' if sl_side == 'BUY' else 'BUY')}, "
                    f"qty={live_qty}) with NO protective stop. State is "
                    "being kept (not purged) so this isn't silently "
                    "forgotten - MANUAL INTERVENTION REQUIRED.",
                    flush=True
                )


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
# FIX 1: the SL is only cancelled AFTER the flatten is confirmed flat. If it
# can't be confirmed, state is NOT purged and the SL is left in place, so a
# failed close never results in a naked, untracked position.
# =============================================================================

def process_close(symbol: str):

    target = clean_symbol(symbol)

    with ENGINE_LOCK:

        if BOT_STATE.get("entry_id"):
            cancel_order(BOT_STATE["entry_id"])

        live_qty, live_side, _ = get_exchange_position_state(target)

        if live_qty > 0:
            opposite = "SELL" if live_side == "BUY" else "BUY"
            flatten_position(target, opposite, live_qty)

            confirmed_flat = wait_until_flat(target, timeout_sec=5)

            if not confirmed_flat:
                print(
                    "[CLOSE ABORTED] Position did not confirm flat. "
                    "Existing stop left in place; state NOT purged.",
                    flush=True
                )
                return

        # Only reached if there was nothing to flatten, or the flatten
        # was confirmed - safe to remove the stop and clear state now.
        if BOT_STATE.get("sl_id"):
            cancel_order(BOT_STATE["sl_id"])

        purge_state()


# =============================================================================
# WEBHOOK
# =============================================================================

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online"}


@app.get("/diag/outbound-ip")
async def diag_outbound_ip():
    """
    TEMP DIAGNOSTIC - remove once the signature-mismatch issue is resolved.
    Reports the IP this container is ACTUALLY egressing from right now, so
    it can be compared against what's whitelisted on Shark Exchange
    (49.37.100.154 / 74.220.52.6). If this doesn't match either one, that
    confirms the outbound IP isn't fixed for this service.
    """
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


# =============================================================================
# START SERVER
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
