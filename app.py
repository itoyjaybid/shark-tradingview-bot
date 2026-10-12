import os
import time
import json
import hmac
import hashlib
import asyncio
import urllib.request
import urllib.error
from email.utils import parsedate_to_datetime
from typing import Optional
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

last_processed_trade_id: Optional[int] = None
clock_offset_ms: int = 0
active_watcher_tasks = {}
active_limit_cids = {}
active_sl_cids = {}

STANDARD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Content-Type": "application/json"
}

# =============================================================================
# SCHEMAS
# =============================================================================
class WebhookPayload(BaseModel):
    secret: str
    symbol: str
    action: str          # "BUY", "SELL", "CLOSE", "CANCEL_ENTRY", "UPDATE_SL"
    order_type: str      # "LIMIT", "MARKET", "CANCEL", "STOP_LIMIT"
    quantity: float
    price: float
    sl_price: float
    sl_buffer: float
    position_side: str
    trade_id: int

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
                print(f"[BOOT] Clock Offset synced via HTTP Date: {clock_offset_ms}ms")
                return
    except Exception as e:
        print(f"[BOOT] Clock sync notice: {e}")
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
        print(f"[SHARK API HTTP ERROR] {err.code}: {body}")
        try:
            return err.code, json.loads(body)
        except Exception:
            return err.code, {"error": body}
    except Exception as e:
        print(f"[SHARK API REQ ERROR] {e}")
        return 500, {"error": str(e)}

async def shark_call(method: str, endpoint: str, query_params: dict = None, body_dict: dict = None):
    return await asyncio.to_thread(shark_send_request, method, endpoint, query_params, body_dict)

# =============================================================================
# EXCHANGE OPERATIONS
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
            if abs(raw_qty) > 0.000001:
                active_positions.append(pos)
    return active_positions

async def cancel_order(client_order_id: str):
    if not client_order_id:
        return
    status, data = await shark_call("DELETE", "/v1/order/delete-order", body_dict={"clientOrderId": str(client_order_id)})
    print(f"[CANCEL ORDER] cid={client_order_id} status={status}")
    return data

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
    elif order_type == "STOP_LIMIT":
        payload["stopPrice"] = clean_num(stop_price, 2)
        payload["price"] = clean_num(price, 2)

    return await shark_call("POST", "/v1/order/place-order", body_dict=payload)

def stop_active_watcher(symbol: str):
    if symbol in active_watcher_tasks and not active_watcher_tasks[symbol].done():
        active_watcher_tasks[symbol].cancel()
        print(f"[WATCHER TERMINATED] Stopped monitoring loop for {symbol}")

# =============================================================================
# PERSISTENT LIMIT ORDER WATCHER (RUNS UNTIL FILLED OR EXITED)
# =============================================================================
async def watch_limit_and_place_sl(symbol: str, client_order_id: str, sl_price: float, sl_buffer: float, side: str, qty: float):
    print(f"[ENTRY WATCHER] Persistent monitor started for Limit Order {client_order_id}.")
    try:
        while True:
            await asyncio.sleep(4)
            positions = await get_position_state(symbol)
            if positions:
                sl_side = "SELL" if side == "BUY" else "BUY"
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
                print(f"[SL ATTACHED] Fill detected! Placed Stop Limit @ {sl_price} (Limit {limit_price}): status={status}")
                return
    except asyncio.CancelledError:
        print(f"[ENTRY WATCHER] Watcher task cancelled.")
        return
    except Exception as e:
        print(f"[ENTRY WATCHER ERROR] {e}")

# =============================================================================
# APPLICATION LIFECYCLE & ROUTES
# =============================================================================
@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(sync_server_clock)

@app.api_route("/", methods=["GET", "HEAD"])
def health_check():
    return {"status": "live", "offset_ms": clock_offset_ms}

@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    global last_processed_trade_id
    
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")
    
    print(f"[WEBHOOK] Action={payload.action} TradeID={payload.trade_id} Qty={payload.quantity} Price={payload.price} SL={payload.sl_price} Buffer={payload.sl_buffer}")
    
    if payload.action in ["BUY", "SELL"] and payload.trade_id == last_processed_trade_id:
        print(f"[DUPLICATE SIGNAL IGNORED] Trade ID {payload.trade_id} already processed.")
        return {"status": "ignored", "reason": "duplicate"}
    
    last_processed_trade_id = payload.trade_id

    # 1. Limit Entry (New Breakout or Immediate Retest)
    if payload.action in ["BUY", "SELL"] and payload.order_type == "LIMIT":
        stop_active_watcher(payload.symbol)
        
        if payload.symbol in active_limit_cids:
            await cancel_order(active_limit_cids[payload.symbol])
            
        status, data = await place_shark_order(
            symbol=payload.symbol,
            side=payload.action,
            order_type="LIMIT",
            qty=payload.quantity,
            price=payload.price
        )
        print(f"[ORDER SUCCESS] status={status} data={data}")
        
        cid = data.get("clientOrderId") or data.get("data", {}).get("clientOrderId") or str(payload.trade_id)
        active_limit_cids[payload.symbol] = str(cid)
        
        # Spawns a persistent watcher loop
        task = asyncio.create_task(
            watch_limit_and_place_sl(payload.symbol, cid, payload.sl_price, payload.sl_buffer, payload.action, payload.quantity)
        )
        active_watcher_tasks[payload.symbol] = task
        return {"status": "limit_placed", "data": data}

    # 2. Dynamic Profit Lock / Trailing SL Update
    elif payload.action == "UPDATE_SL":
        positions = await get_position_state(payload.symbol)
        if positions:
            pos = positions[0]
            pos_amt = abs(float(pos.get("positionAmount") or pos.get("quantity") or 0.0))
            pos_side = str(pos.get("positionType") or pos.get("side") or "").upper()
            sl_side = "SELL" if "BUY" in pos_side else "BUY"
            
            if payload.symbol in active_sl_cids:
                await cancel_order(active_sl_cids[payload.symbol])
            
            limit_price = (payload.sl_price - payload.sl_buffer) if sl_side == "SELL" else (payload.sl_price + payload.sl_buffer)
            status, data = await place_shark_order(
                symbol=payload.symbol,
                side=sl_side,
                order_type="STOP_LIMIT",
                qty=pos_amt,
                price=limit_price,
                stop_price=payload.sl_price,
                reduce_only=True
            )
            cid = data.get("clientOrderId") or data.get("data", {}).get("clientOrderId")
            if cid:
                active_sl_cids[payload.symbol] = str(cid)
            print(f"[PROFIT LOCK APPLIED] Modified Stop Limit to {payload.sl_price}: status={status}")
            return {"status": "sl_updated", "data": data}
        return {"status": "ignored", "reason": "no_open_position"}

    # 3. Market Exit / Emergency Close
    elif payload.action == "CLOSE" or payload.order_type == "MARKET":
        stop_active_watcher(payload.symbol)
        
        if payload.symbol in active_sl_cids:
            await cancel_order(active_sl_cids.pop(payload.symbol))
            
        if payload.symbol in active_limit_cids:
            await cancel_order(active_limit_cids.pop(payload.symbol))
            
        positions = await get_position_state(payload.symbol)
        if positions:
            pos = positions[0]
            pos_amt = abs(float(pos.get("positionAmount") or pos.get("quantity") or 0.0))
            pos_side = str(pos.get("positionType") or pos.get("side") or "").upper()
            close_side = "SELL" if "BUY" in pos_side else "BUY"
            
            status, data = await place_shark_order(
                symbol=payload.symbol,
                side=close_side,
                order_type="MARKET",
                qty=pos_amt,
                reduce_only=True
            )
            print(f"[MARKET EXIT] Closed position of {pos_amt} {payload.symbol}")
            return {"status": "closed", "data": data}
        else:
            print("[MARKET EXIT IGNORED] Position is already flat.")
            return {"status": "ignored", "reason": "already_flat"}

    # 4. Cancel Pending Entry
    elif payload.action == "CANCEL_ENTRY":
        stop_active_watcher(payload.symbol)
        if payload.symbol in active_limit_cids:
            await cancel_order(active_limit_cids.pop(payload.symbol))
        return {"status": "cancelled"}

    return {"status": "unhandled_action"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000)
