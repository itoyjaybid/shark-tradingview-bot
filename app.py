import os
import time
import hmac
import hashlib
import asyncio
from typing import Optional
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import httpx

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
# SHARK EXCHANGE SIGNATURE & API UTILITIES
# =============================================================================
def get_timestamp() -> int:
    return int(time.time() * 1000) + clock_offset_ms

def generate_signature(query_string: str) -> str:
    return hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

async def sync_server_clock():
    global clock_offset_ms
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{BASE_URL}/api/v1/time")
            if resp.status_code == 200:
                server_time = resp.json().get("serverTime", int(time.time() * 1000))
                local_time = int(time.time() * 1000)
                clock_offset_ms = server_time - local_time
                print(f"[BOOT] Clock Offset = {clock_offset_ms}ms")
    except Exception as e:
        print(f"[BOOT] Failed to sync clock: {e}")

async def shark_request(method: str, endpoint: str, params: dict = None, json_data: dict = None):
    if params is None:
        params = {}
    
    params["timestamp"] = get_timestamp()
    query_str = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    signature = generate_signature(query_str)
    
    headers = {
        "X-MBX-APIKEY": API_KEY,
        "Content-Type": "application/json"
    }
    
    url = f"{BASE_URL}{endpoint}?{query_str}&signature={signature}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        if method.upper() == "GET":
            return await client.get(url, headers=headers)
        elif method.upper() == "POST":
            return await client.post(url, headers=headers, json=json_data)
        elif method.upper() == "DELETE":
            return await client.delete(url, headers=headers)

# =============================================================================
# EXCHANGE HELPERS
# =============================================================================
async def get_position_state(symbol: str):
    """Returns currently open positions for the symbol."""
    try:
        resp = await shark_request("GET", "/fapi/v2/positionRisk", {"symbol": symbol})
        if resp.status_code == 200:
            positions = resp.json()
            active = [p for p in positions if abs(float(p.get("positionAmt", 0))) > 0]
            print(f"[POSITION RAW] target={symbol} -> {active}")
            return active
    except Exception as e:
        print(f"[POSITION ERROR] {e}")
    return []

async def cancel_resting_orders(symbol: str):
    """Cancels all open orders for the symbol."""
    try:
        resp = await shark_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        print(f"[CANCEL ALL] symbol={symbol} status={resp.status_code}")
        return resp.json()
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

    resp = await shark_request("POST", "/fapi/v1/order", params=params)
    return resp

# =============================================================================
# BACKGROUND LIMIT ORDER WATCHER
# =============================================================================
async def watch_limit_and_place_sl(symbol: str, client_order_id: str, sl_price: float, side: str, qty: float):
    """Polls until limit entry fills, then places the native stop loss order."""
    print(f"[ENTRY WATCHER] Monitoring Limit Order {client_order_id} until executed.")
    for _ in range(360):  # Monitors for up to 30 minutes
        await asyncio.sleep(5)
        positions = await get_position_state(symbol)
        
        # If position filled, place SL on exchange
        if positions:
            sl_side = "SELL" if side == "BUY" else "BUY"
            sl_resp = await place_order(
                symbol=symbol,
                side=sl_side,
                order_type="STOP_MARKET",
                qty=qty,
                stop_price=sl_price
            )
            print(f"[SL ATTACHED] Placed Stop Market @ {sl_price}: {sl_resp.status_code}")
            return
    print(f"[ENTRY WATCHER] Order cancelled or superseded.")

# =============================================================================
# APPLICATION LIFECYCLE & WEBHOOK ROUTE
# =============================================================================
@app.on_event("startup")
async def startup_event():
    await sync_server_clock()

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
    
    # 2. EXACT DUPLICATE CHECK (Replaces the broken '<=' check)
    if payload.trade_id == last_processed_trade_id:
        print(f"[DUPLICATE SIGNAL IGNORED] Trade ID {payload.trade_id} already executed.")
        return {"status": "ignored", "reason": "duplicate"}
    
    # Update last processed trade id
    last_processed_trade_id = payload.trade_id

    # 3. Process Execution Actions
    if payload.action in ["BUY", "SELL"] and payload.order_type == "LIMIT":
        # Cancel previous resting orders before placing a fresh entry
        await cancel_resting_orders(payload.symbol)
        
        resp = await place_order(
            symbol=payload.symbol,
            side=payload.action,
            order_type="LIMIT",
            qty=payload.quantity,
            price=payload.price
        )
        
        data = resp.json() if resp.status_code == 200 else {}
        print(f"[ORDER SUCCESS] {data}")
        
        client_id = data.get("clientOrderId", str(payload.trade_id))
        task = asyncio.create_task(
            watch_limit_and_place_sl(payload.symbol, client_id, payload.sl_price, payload.action, payload.quantity)
        )
        active_watcher_tasks[payload.symbol] = task
        return {"status": "limit_placed", "data": data}

    elif payload.action == "CLOSE" or payload.order_type == "MARKET":
        # Emergency exit / Stop Loss blowout exit
        positions = await get_position_state(payload.symbol)
        if positions:
            pos_amt = float(positions[0].get("positionAmt", 0))
            close_side = "SELL" if pos_amt > 0 else "BUY"
            await cancel_resting_orders(payload.symbol)
            resp = await place_order(
                symbol=payload.symbol,
                side=close_side,
                order_type="MARKET",
                qty=abs(pos_amt)
            )
            print(f"[MARKET EXIT] Closed position of {pos_amt} {payload.symbol}")
            return {"status": "closed", "data": resp.json() if resp.status_code == 200 else {}}
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
