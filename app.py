import os
import time
import json
import hmac
import hashlib
import asyncio
import urllib.request
import urllib.error
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

# In-memory tracking
last_processed_trade_id: Optional[int] = None
clock_offset_ms: int = 0
active_watcher_tasks = {}

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
    position_side: str   # "BUY", "SELL", "NONE"
    trade_id: int

# =============================================================================
# SHARK EXCHANGE SIGNATURE & API UTILITIES (Standard Library urllib)
# =============================================================================
def get_timestamp() -> int:
    return int(time.time() * 1000) + clock_offset_ms

def generate_signature(query_string: str) -> str:
    return hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

def sync_server_clock():
    global clock_offset_ms
    try:
        req = urllib.request.Request(f"{BASE_URL}/api/v1/time", method="GET")
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                server_time = data.get("serverTime", int(time.time() * 1000))
                local_time = int(time.time() * 1000)
                clock_offset_ms = server_time - local_time
                print(f"[BOOT] Clock Offset = {clock_offset_ms}ms")
    except Exception as e:
        print(f"[BOOT] Failed to sync clock: {e}")

def shark_request_sync(method: str, endpoint: str, params: dict = None, json_data: dict = None):
    if params is None:
        params = {}
    
    params["timestamp"] = get_timestamp()
    query_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    signature = generate_signature(query_str)
    
    url = f"{BASE_URL}{endpoint}?{query_str}&signature={signature}"
    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    data_bytes = None
    if json_data is not None:
        data_bytes = json.dumps(json_data).encode("utf-8")
        
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

async def shark_request(method: str, endpoint: str, params: dict = None, json_data: dict = None):
    return await asyncio.to_thread(shark_request_sync, method, endpoint, params, json_data)

# =============================================================================
# EXCHANGE HELPERS
# =============================================================================
async def get_position_state(symbol: str):
    """Returns currently open positions for the symbol."""
    try:
        status, data = await shark_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        if status == 200 and isinstance(data, list):
            active = [p for p in data if abs(float(p.get("positionAmt", 0))) > 0]
            print(f"[POSITION RAW] target={symbol} -> {active}")
            return active
    except Exception as e:
        print(f"[POSITION ERROR] {e}")
    return []

async def cancel_resting_orders(symbol: str):
    """Cancels all open orders for the symbol."""
    try:
        status, data = await shark_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        print(f"[CANCEL ALL] symbol={symbol} status={status}")
        return data
    except Exception as e:
        print(f"[CANCEL ERROR] {e}")

async def place_order(symbol: str, side: str, order_type: str, qty: float, price: float = None, stop_price: float = None):
    params = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "quantity": qty
    }
    if order_type == "LIMIT":
        params["price"] = price
        params["timeInForce"] = "GTC"
    elif order_type in ["STOP_MARKET", "STOP"]:
        params["stopPrice"] = stop_price

    return await shark_request("POST", "/fapi/v1/order", params=params)

# =============================================================================
# BACKGROUND LIMIT ORDER WATCHER
# =============================================================================
async def watch_limit_and_place_sl(symbol: str, client_order_id: str, sl_price: float, side: str, qty: float):
    print(f"[ENTRY WATCHER] Monitoring Limit Order {client_order_id} until executed.")
    for _ in range(360):
        await asyncio.sleep(5)
        positions = await get_position_state(symbol)
        if positions:
            sl_side = "SELL" if side == "BUY" else "BUY"
            status, _ = await place_order(
                symbol=symbol,
                side=sl_side,
                order_type="STOP_MARKET",
                qty=qty,
                stop_price=sl_price
            )
            print(f"[SL ATTACHED] Placed Stop Market @ {sl_price}: status={status}")
            return
    print(f"[ENTRY WATCHER] Order cancelled or superseded.")

# =============================================================================
# APPLICATION LIFECYCLE & WEBHOOK ROUTE
# =============================================================================
@app.on_event("startup")
async def startup_event():
    await asyncio.to_thread(sync_server_clock)

@app.get("/")
def health_check():
    return {"status": "live", "offset_ms": clock_offset_ms}

@app.post("/webhook")
async def receive_webhook(payload: WebhookPayload):
    global last_processed_trade_id
    
    # 1. Authenticate Secret
    if payload.secret != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid secret")
    
    print(f"[WEBHOOK] Action={payload.action} TradeID={payload.trade_id} Qty={payload.quantity} Price={payload.price} SL={payload.sl_price} Buffer={payload.sl_buffer}")
    
    # 2. Duplicate Check: Block only if identical to the immediate preceding trade
    if payload.trade_id == last_processed_trade_id:
        print(f"[DUPLICATE SIGNAL IGNORED] Trade ID {payload.trade_id} already executed.")
        return {"status": "ignored", "reason": "duplicate"}
    
    last_processed_trade_id = payload.trade_id

    # 3. Process Execution Actions
    if payload.action in ["BUY", "SELL"] and payload.order_type == "LIMIT":
        await cancel_resting_orders(payload.symbol)
        
        status, data = await place_order(
            symbol=payload.symbol,
            side=payload.action,
            order_type="LIMIT",
            qty=payload.quantity,
            price=payload.price
        )
        
        print(f"[ORDER SUCCESS] status={status} data={data}")
        
        client_id = data.get("clientOrderId", str(payload.trade_id)) if isinstance(data, dict) else str(payload.trade_id)
        task = asyncio.create_task(
            watch_limit_and_place_sl(payload.symbol, client_id, payload.sl_price, payload.action, payload.quantity)
        )
        active_watcher_tasks[payload.symbol] = task
        return {"status": "limit_placed", "data": data}

    elif payload.action == "CLOSE" or payload.order_type == "MARKET":
        positions = await get_position_state(payload.symbol)
        if positions:
            pos_amt = float(positions[0].get("positionAmt", 0))
            close_side = "SELL" if pos_amt > 0 else "BUY"
            await cancel_resting_orders(payload.symbol)
            status, data = await place_order(
                symbol=payload.symbol,
                side=close_side,
                order_type="MARKET",
                qty=abs(pos_amt)
            )
            print(f"[MARKET EXIT] Closed position of {pos_amt} {payload.symbol}")
            return {"status": "closed", "data": data}
        else:
            print("[MARKET EXIT IGNORED] Position is already flat.")
            return {"status": "ignored", "reason": "already_flat"}

    elif payload.action == "CANCEL_ENTRY":
        await cancel_resting_orders(payload.symbol)
        return {"status": "cancelled"}

    return {"status": "unhandled_action"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=10000)
