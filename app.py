import os
import time
import json
import hmac
import hashlib
import asyncio
import urllib.request
import urllib.error
from email.utils import parsedate_to_datetime
from typing import Optional, Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Shark Exchange TradingView Webhook Bridge")

# =============================================================================
# ENVIRONMENT CONFIGURATION
# =============================================================================
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "MY_SECRET_KEY")
API_KEY = os.getenv("SHARK_API_KEY", "")
API_SECRET = os.getenv("SHARK_API_SECRET", "")
BASE_URL = os.getenv("SHARK_BASE_URL", "https://api.sharkexchange.in")

clock_offset_ms: int = 0
active_watcher_tasks = {}
active_limit_cids = {}
active_sl_cids = {}

# Thread lock and action deduplication to prevent duplicate orders
action_lock = asyncio.Lock()
recent_actions = {}

STANDARD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Content-Type": "application/json"
}

# =============================================================================
# FLEXIBLE SCHEMAS
# =============================================================================
class WebhookPayload(BaseModel):
    secret: Optional[str] = None
    passphrase: Optional[str] = None
    symbol: str = "BTCUSDT"
    action: str                                  # BUY, SELL, CLOSE, CANCEL_ENTRY, UPDATE_SL
    order_type: Optional[str] = "LIMIT"          # LIMIT, MARKET, CANCEL, STOP_LIMIT
    quantity: Optional[float] = None
    qty: Optional[float] = None
    price: Optional[float] = None
    sl_price: Optional[float] = None
    sl: Optional[float] = None
    sl_buffer: Optional[float] = None
    buffer: Optional[float] = None
    position_side: Optional[str] = None
    trade_id: Optional[Any] = 0

    class Config:
        extra = "allow"

    def get_qty(self) -> float:
        val = self.quantity if self.quantity is not None else self.qty
        return float(val) if val is not None else 0.01

    def get_sl(self) -> float:
        val = self.sl_price if self.sl_price is not None else self.sl
        return float(val) if val is not None else 0.0

    def get_buffer(self) -> float:
        val = self.sl_buffer if self.sl_buffer is not None else self.buffer
        return float(val) if val is not None else 15.0

    def get_secret(self) -> str:
        return self.secret or self.passphrase or ""

# =============================================================================
# SHARK EXCHANGE SIGNATURE & API UTILITIES
# =============================================================================
def get_synced_time() -> int:
    return int(time.time() * 1000) + clock_offset_ms

def generate_signature(api_secret: str, data_to_sign: str) -> str:
    return hmac.new(
        api_secret.encode("utf-8"),
        data_to_sign.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def clean_num(val: float, precision: int = 2):
    if val is None:
        return 0
    rounded = round(float(val), precision)
    return int(rounded) if rounded.is_integer() else rounded

def clean_symbol(sym: str) -> str:
    return sym.replace("-", "").replace("_", "").replace(".P", "").replace(".p", "").replace("/", "").upper()

def sync_server_clock():
    global clock_offset_ms
    url = f"{BASE_URL}/v1/market/ticker24Hr/btcusdt"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": STANDARD_HEADERS["User-Agent"]}, method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            date_hdr = resp.headers.get("Date")
            if date_hdr:
                server_dt = parsedate_to_datetime(date_hdr)
                server_ts = int(server_dt.timestamp() * 1000)
                local_ts = int(time.time() * 1000)
                clock_offset_ms = server_ts - local_ts
                print(f"[BOOT] Clock Offset synced via HTTP Date: {clock_offset_ms}ms", flush=True)
                return
    except Exception as e:
        print(f"[BOOT] Clock sync notice: {e}", flush=True)
    clock_offset_ms = 0

def shark_send_request(method: str, endpoint: str, query_params: dict = None, body_dict: dict = None):
    ts = str(get_synced_time())
    
    if method.upper() in ["POST", "PATCH", "DELETE"]:
        payload = body_dict or {}
        payload["timestamp"] = ts
        data_to_sign = json.dumps(payload, separators=(',', ':'))
        sig = generate_signature(API_SECRET, data_to_sign)
        headers = {
            **STANDARD_HEADERS,
            "api-key": API_KEY,
            "signature": sig
        }
        url = f"{BASE_URL}{endpoint}"
        data_bytes = data_to_sign.encode("utf-8")
    else:
        params = query_params or {}
        params["timestamp"] = ts
        query_str = "&".join(f"{k}={v}" for k, v in params.items())
        sig = generate_signature(API_SECRET, query_str)
        headers = {
            **STANDARD_HEADERS,
            "api-key": API_KEY,
            "signature": sig
        }
        url = f"{BASE_URL}{endpoint}?{query_str}"
        data_bytes = None

    req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8")
        try:
            parsed_err = json.loads(body)
        except Exception:
            parsed_err = {"error": body}
        if err.code != 400 or str(parsed_err.get("error")) != "3060":
            print(f"[SHARK API HTTP ERROR] {err.code}: {body}", flush=True)
        return err.code, parsed_err
    except Exception as e:
        print(f"[SHARK API REQ ERROR] {e}", flush=True)
        return 500, {"error": str(e)}

async def shark_call(method: str, endpoint: str, query_params: dict = None, body_dict: dict = None):
    return await asyncio.to_thread(shark_send_request, method, endpoint, query_params, body_dict)

# =============================================================================
# EXCHANGE OPERATIONS & TOTAL PURGING
# =============================================================================
async def get_position_state(symbol: str):
    target = clean_symbol(symbol)
    params = {
        "sortOrder": "desc",
        "pageSize": "100",
        "symbol": target
    }
    status, res = await shark_call("GET", "/v1/positions/OPEN", query_params=params)
    if status != 200:
        return []
    
    items = res if isinstance(res, list) else res.get("data", [])
    if isinstance(items, dict):
        items = [items]
        
    active_positions = []
    for pos in items:
        if not isinstance(pos, dict):
            continue
        sym = str(pos.get("contractPair") or pos.get("symbol") or "").upper()
        if target in sym:
            raw_qty = float(pos.get("positionAmount") or pos.get("quantity") or pos.get("positionAmt") or 0.0)
            if abs(raw_qty) > 0.0001:
                active_positions.append(pos)
    return active_positions

async def cancel_order(client_order_id: str):
    if not client_order_id:
        return None
    status, data = await shark_call("DELETE", "/v1/order/delete-order", body_dict={"clientOrderId": str(client_order_id)})
    if status in [200, 204]:
        print(f"[CANCEL ORDER] cid={client_order_id} status={status}", flush=True)
    return data

async def cancel_all_exchange_open_orders(symbol: str):
    """Queries Shark orderbook directly and cancels every resting limit/stop order."""
    target = clean_symbol(symbol)
    status, res = await shark_call("GET", "/v1/order/open-orders", query_params={"symbol": target, "pageSize": "100"})
    if status == 200:
        orders = res if isinstance(res, list) else res.get("data", [])
        if isinstance(orders, dict):
            orders = [orders]
        for ord_item in orders:
            cid = ord_item.get("clientOrderId") or ord_item.get("id")
            if cid:
                await cancel_order(str(cid))
                print(f"[PURGED ORDER] Cancelled open order {cid} on {target}", flush=True)

async def cancel_all_tracked_orders(symbol: str):
    if symbol in active_sl_cids:
        await cancel_order(active_sl_cids.pop(symbol))
    if symbol in active_limit_cids:
        await cancel_order(active_limit_cids.pop(symbol))
    await cancel_all_exchange_open_orders(symbol)

async def place_shark_order(symbol: str, side: str, order_type: str, qty: float, price: float = None, stop_price: float = None, reduce_only: bool = False):
    target = clean_symbol(symbol)
    payload = {
        "deviceType": "WEB",
        "marginAsset": "INR",
        "placeType": "ORDER_FORM",
        "quantity": clean_num(qty, 4),
        "reduceOnly": reduce_only,
        "side": side,
        "symbol": target,
        "type": order_type
    }
    if order_type == "LIMIT":
        payload["price"] = clean_num(price, 2)
    elif order_type in ["STOP_LIMIT", "STOP"]:
        payload["stopPrice"] = clean_num(stop_price, 2)
        payload["price"] = clean_num(price, 2)

    return await shark_call("POST", "/v1/order/place-order", body_dict=payload)

def stop_active_watcher(symbol: str):
    if symbol in active_watcher_tasks and not active_watcher_tasks[symbol].done():
        active_watcher_tasks[symbol].cancel()
        print(f"[WATCHER TERMINATED] Stopped monitoring loop for {symbol}", flush=True)

# =============================================================================
# CONTINUOUS LIFECYCLE WATCHER (Entry Monitor + Manual Exit Flat Guard)
# =============================================================================
async def watch_limit_and_place_sl(symbol: str, client_order_id: str, sl_price: float, sl_buffer: float, side: str, qty: float):
    print(f"[LIFECYCLE WATCHER] Started monitoring loop for {symbol} (Order {client_order_id}).", flush=True)
    sl_placed = False
    try:
        # Phase 1: Wait for entry fill, then place Stop-Limit
        while not sl_placed:
            await asyncio.sleep(1.5)
            positions = await get_position_state(symbol)
            if positions:
                pos = positions[0]
                pos_side = str(pos.get("positionType") or pos.get("side") or "").upper()
                is_long = ("BUY" in pos_side) or ("LONG" in pos_side) or (side.upper() == "BUY")
                sl_side = "SELL" if is_long else "BUY"
                limit_price = (sl_price - sl_buffer) if sl_side == "SELL" else (sl_price + sl_buffer)
                
                status, data = await place_shark_order(
                    symbol=symbol,
                    side=sl_side,
                    order_type="STOP_LIMIT",
                    qty=qty,
                    price=limit_price,
                    stop_price=sl_price,
                    reduce_only=True
                )
                cid = data.get("clientOrderId") or data.get("data", {}).get("clientOrderId")
                if cid:
                    active_sl_cids[symbol] = str(cid)
                print(f"[SL ATTACHED] Fill detected! Placed Stop Limit {sl_side} @ {sl_price} (Limit {limit_price}): status={status}", flush=True)
                sl_placed = True

        # Phase 2: Active Trade Monitor — Auto-purge if closed manually on exchange
        print(f"[ACTIVE POSITION GUARD] Monitoring {symbol} position. Will auto-cancel SL if closed manually.", flush=True)
        while True:
            await asyncio.sleep(2.0)
            positions = await get_position_state(symbol)
            if not positions:
                print(f"[MANUAL EXIT DETECTED] Position is flat! Auto-cancelling all resting SL/buffers on {symbol}...", flush=True)
                await cancel_all_exchange_open_orders(symbol)
                if symbol in active_sl_cids:
                    del active_sl_cids[symbol]
                return

    except asyncio.CancelledError:
        print(f"[LIFECYCLE WATCHER] Task cancelled cleanly.", flush=True)
        return
    except Exception as e:
        print(f"[LIFECYCLE WATCHER ERROR] {e}", flush=True)

# =============================================================================
# APPLICATION ROUTES
# =============================================================================
@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(sync_server_clock)

@app.api_route("/", methods=["GET", "HEAD"])
def health_check():
    return {"status": "live", "offset_ms": clock_offset_ms}

@app.get("/purge")
async def manual_purge(symbol: str = "BTCUSDT"):
    stop_active_watcher(symbol)
    await cancel_all_tracked_orders(symbol)
    return {"status": "purged", "symbol": symbol}

@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    expected_secret = WEBHOOK_SECRET
    provided_secret = payload.get_secret()
    if expected_secret and expected_secret != "MY_SECRET_KEY" and provided_secret != expected_secret:
        raise HTTPException(status_code=401, detail="Invalid secret")

    action = payload.action.upper()
    trade_id = str(payload.trade_id)
    symbol = clean_symbol(payload.symbol)
    qty = payload.get_qty()
    sl_price = payload.get_sl()
    sl_buffer = payload.get_buffer()
    price = payload.price
    now = time.time()

    # Deduplication check: drop duplicate signals arriving within 4 seconds
    dedup_key = f"{symbol}_{action}_{trade_id}"
    if dedup_key in recent_actions and (now - recent_actions[dedup_key]) < 4.0:
        print(f"[DUPLICATE IGNORED] Discarded duplicate webhook: {dedup_key}", flush=True)
        return {"status": "ignored", "reason": "duplicate"}
    recent_actions[dedup_key] = now

    for k in list(recent_actions.keys()):
        if now - recent_actions[k] > 60.0:
            del recent_actions[k]

    print(f"[WEBHOOK] Action={action} TradeID={trade_id} Qty={qty} Price={price} SL={sl_price} Buffer={sl_buffer}", flush=True)

    async with action_lock:
        # 1. Limit Entry Order
        if action in ["BUY", "SELL"] and payload.order_type in ["LIMIT", None]:
            stop_active_watcher(symbol)
            await cancel_all_tracked_orders(symbol)
                
            status, data = await place_shark_order(
                symbol=symbol,
                side=action,
                order_type="LIMIT",
                qty=qty,
                price=price
            )
            print(f"[ORDER SUCCESS] status={status} data={data}", flush=True)
            
            cid = data.get("clientOrderId") or data.get("data", {}).get("clientOrderId") or str(trade_id)
            active_limit_cids[symbol] = str(cid)
            
            task = asyncio.create_task(
                watch_limit_and_place_sl(symbol, cid, sl_price, sl_buffer, action, qty)
            )
            active_watcher_tasks[symbol] = task
            return {"status": "limit_placed", "data": data}

        # 2. Dynamic Trailing SL / Profit Lock
        elif action == "UPDATE_SL":
            positions = await get_position_state(symbol)
            if positions:
                pos = positions[0]
                pos_amt = abs(float(pos.get("positionAmount") or pos.get("quantity") or pos.get("positionAmt") or 0.0))
                
                if pos_amt > 0.0001:
                    pos_side = str(pos.get("positionType") or pos.get("side") or "").upper()
                    # Robust check: matches BUY, LONG, or payload.position_side
                    is_long = ("BUY" in pos_side) or ("LONG" in pos_side) or (payload.position_side and "BUY" in payload.position_side.upper())
                    sl_side = "SELL" if is_long else "BUY"
                    
                    if symbol in active_sl_cids:
                        await cancel_order(active_sl_cids.pop(symbol))
                    
                    limit_price = (sl_price - sl_buffer) if sl_side == "SELL" else (sl_price + sl_buffer)
                    status, data = await place_shark_order(
                        symbol=symbol,
                        side=sl_side,
                        order_type="STOP_LIMIT",
                        qty=pos_amt,
                        price=limit_price,
                        stop_price=sl_price,
                        reduce_only=True
                    )
                    cid = data.get("clientOrderId") or data.get("data", {}).get("clientOrderId")
                    if cid:
                        active_sl_cids[symbol] = str(cid)
                    print(f"[PROFIT LOCK APPLIED] Modified Stop Limit to {sl_price} ({sl_side}): status={status}", flush=True)
                    return {"status": "sl_updated", "data": data}
            
            print(f"[UPDATE_SL IGNORED] No active position for {symbol}", flush=True)
            return {"status": "ignored", "reason": "no_open_position"}

        # 3. Market Exit & Order Purge
        elif action == "CLOSE" or payload.order_type == "MARKET":
            stop_active_watcher(symbol)
            await cancel_all_tracked_orders(symbol)
                
            positions = await get_position_state(symbol)
            if positions:
                pos = positions[0]
                pos_amt = abs(float(pos.get("positionAmount") or pos.get("quantity") or pos.get("positionAmt") or 0.0))
                
                if pos_amt > 0.0001:
                    pos_side = str(pos.get("positionType") or pos.get("side") or "").upper()
                    # Robust check: matches BUY, LONG, or payload.position_side
                    is_long = ("BUY" in pos_side) or ("LONG" in pos_side) or (payload.position_side and "BUY" in payload.position_side.upper())
                    close_side = "SELL" if is_long else "BUY"
                    
                    status, data = await place_shark_order(
                        symbol=symbol,
                        side=close_side,
                        order_type="MARKET",
                        qty=pos_amt,
                        reduce_only=True
                    )
                    print(f"[MARKET EXIT SUCCESS] Closed position of {pos_amt} {symbol} with {close_side}: status={status}", flush=True)
                    await cancel_all_exchange_open_orders(symbol)
                    return {"status": "closed", "data": data}
                else:
                    print("[MARKET EXIT IGNORED] Position is already 0.", flush=True)
                    return {"status": "ignored", "reason": "already_flat"}
            else:
                print("[MARKET EXIT IGNORED] Position is already flat.", flush=True)
                return {"status": "ignored", "reason": "already_flat"}

        # 4. Cancel Pending Entry
        elif action == "CANCEL_ENTRY":
            stop_active_watcher(symbol)
            await cancel_all_tracked_orders(symbol)
            return {"status": "cancelled"}

    return {"status": "unhandled_action"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
