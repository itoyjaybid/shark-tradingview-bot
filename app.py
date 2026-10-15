import os
import sys
import time
import json
import hmac
import hashlib
import asyncio
from typing import Optional, Dict, Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from fastapi import FastAPI, Request as FastAPIRequest, HTTPException, status
from pydantic import BaseModel, Field
import uvicorn

# ==============================================================================
# CONFIGURATION & CREDENTIALS
# ==============================================================================
API_KEY = os.getenv("SHARK_API_KEY", "")
API_SECRET = os.getenv("SHARK_API_SECRET", "")
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "")
BASE_URL = os.getenv("SHARK_BASE_URL", "https://api.sharkexchange.in")

if not API_KEY or not API_SECRET:
    print("[CRITICAL WARNING] SHARK_API_KEY or SHARK_API_SECRET is missing!", flush=True)

app = FastAPI(title="Shark TradingView Bridge")

# Thread safety lock & action deduplication cache
action_lock = asyncio.Lock()
recent_actions: Dict[str, float] = {}

# Active trade tracking
active_limit_cids: Dict[str, str] = {}       # symbol -> entry clientOrderId
active_sl_cids: Dict[str, str] = {}          # symbol -> stop loss clientOrderId
watcher_tasks: Dict[str, asyncio.Task] = {}  # symbol -> background Task


# ==============================================================================
# PYDANTIC MODEL
# ==============================================================================
class WebhookPayload(BaseModel):
    passphrase: Optional[str] = None
    action: str = Field(..., description="BUY, SELL, CANCEL_ENTRY, UPDATE_SL, CLOSE")
    symbol: str = "BTCUSDT"
    trade_id: Optional[str] = None
    qty: float = Field(0.01, gt=0)
    price: Optional[float] = None
    sl: Optional[float] = 0.0
    buffer: Optional[float] = 15.0


# ==============================================================================
# SHARK EXCHANGE SIGNED API CLIENT
# ==============================================================================
def sign_query(query_params: dict, secret: str) -> str:
    sorted_query = urlencode(sorted(query_params.items()))
    signature = hmac.new(
        secret.encode("utf-8"),
        sorted_query.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return f"{sorted_query}&signature={signature}"


async def shark_request(method: str, path: str, params: Optional[dict] = None) -> tuple[int, dict]:
    """Execute non-blocking signed HTTP requests using standard urllib in threadpool."""
    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 10000

    query_str = sign_query(params, API_SECRET)

    if method.upper() in ["GET", "DELETE"]:
        full_url = f"{BASE_URL}{path}?{query_str}"
        body_bytes = None
    else:
        # POST / PUT sends form-encoded signed body
        full_url = f"{BASE_URL}{path}"
        body_bytes = query_str.encode("utf-8")

    def _sync_call():
        req = Request(full_url, data=body_bytes, method=method.upper())
        req.add_header("X-SHARK-APIKEY", API_KEY)
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        req.add_header("User-Agent", "SharkTradingBot/2.0")

        try:
            with urlopen(req, timeout=10) as resp:
                status_code = resp.getcode()
                resp_data = resp.read().decode("utf-8")
                return status_code, json.loads(resp_data) if resp_data else {}
        except HTTPError as e:
            err_data = e.read().decode("utf-8")
            try:
                parsed_err = json.loads(err_data)
            except Exception:
                parsed_err = {"raw": err_data}
            return e.code, parsed_err
        except URLError as e:
            return 503, {"error": "ConnectionError", "details": str(e.reason)}
        except Exception as e:
            return 500, {"error": "InternalException", "details": str(e)}

    return await asyncio.to_thread(_sync_call)


# ==============================================================================
# HELPER FUNCTIONS & EXCHANGE ACTIONS
# ==============================================================================
async def get_active_position(symbol: str) -> dict:
    """Fetch position details: side (LONG/SHORT/FLAT) and absolute size."""
    status_code, data = await shark_request("GET", "/fapi/v1/positions", {"symbol": symbol})
    if status_code == 200:
        positions = data if isinstance(data, list) else data.get("positions", [])
        for pos in positions:
            if pos.get("symbol") == symbol:
                size = float(pos.get("positionAmount", pos.get("size", 0.0)))
                side = "LONG" if size > 0 else ("SHORT" if size < 0 else "FLAT")
                return {"side": side, "size": abs(size), "raw": pos}
    return {"side": "FLAT", "size": 0.0, "raw": {}}


async def cancel_order_safe(symbol: str, client_order_id: str) -> None:
    """Cancel order by clientOrderId and ignore 3060 (Order not found)."""
    if not client_order_id:
        return
    status_code, data = await shark_request(
        "DELETE",
        "/fapi/v1/order",
        {"symbol": symbol, "clientOrderId": client_order_id}
    )
    if status_code in [200, 204]:
        print(f"[CANCEL ORDER] cid={client_order_id} status={status_code}", flush=True)
    elif status_code == 400 and str(data.get("error")) == "3060":
        # Order already filled or already removed; normal operation
        pass
    else:
        print(f"[SHARK API HTTP ERROR] {status_code}: {json.dumps(data)}", flush=True)


async def cancel_all_symbol_orders(symbol: str) -> None:
    """Cancel both tracked and resting orders for the symbol."""
    if symbol in active_limit_cids:
        await cancel_order_safe(symbol, active_limit_cids.pop(symbol))
    if symbol in active_sl_cids:
        await cancel_order_safe(symbol, active_sl_cids.pop(symbol))


async def place_protective_stop(symbol: str, position_side: str, qty: float, sl_price: float, buffer: float) -> Optional[str]:
    """Place a Stop Limit order with reduceOnly: True."""
    cid = f"{uuid_tag()}-sl-sha"

    if position_side == "LONG":
        side = "SELL"
        stop_price = round(sl_price, 1)
        limit_price = round(sl_price - buffer, 1)
    else:
        side = "BUY"
        stop_price = round(sl_price, 1)
        limit_price = round(sl_price + buffer, 1)

    payload = {
        "symbol": symbol,
        "side": side,
        "type": "STOP",
        "stopPrice": stop_price,
        "price": limit_price,
        "orderAmount": qty,
        "clientOrderId": cid,
        "reduceOnly": "true"
    }

    status_code, data = await shark_request("POST", "/fapi/v1/order", payload)
    if status_code in [200, 201]:
        active_sl_cids[symbol] = cid
        print(f"[SL ATTACHED] Placed Stop Limit @ {stop_price} (Limit {limit_price}) with reduceOnly: status={status_code}", flush=True)
        return cid
    else:
        print(f"[SL ERROR] Failed to place Stop Limit: {status_code} {json.dumps(data)}", flush=True)
        return None


def uuid_tag() -> str:
    return hashlib.sha256(f"{time.time()}_{os.urandom(8)}".encode()).hexdigest()[:20]


# ==============================================================================
# PERSISTENT ENTRY WATCHER
# ==============================================================================
async def persistent_entry_watcher(symbol: str, entry_cid: str, entry_side: str, qty: float, sl: float, buffer: float):
    """Monitors entry limit order until filled or cancelled."""
    print(f"[ENTRY WATCHER] Persistent monitor started for Limit Order {entry_cid}.", flush=True)
    expected_pos_side = "LONG" if entry_side == "BUY" else "SHORT"

    try:
        while True:
            await asyncio.sleep(2.5)

            # Check if order filled by monitoring open positions
            pos = await get_active_position(symbol)
            if pos["side"] == expected_pos_side and pos["size"] >= (qty * 0.95):
                print(f"[FILL DETECTED] Position confirmed: {pos['side']} {pos['size']} {symbol}", flush=True)
                active_limit_cids.pop(symbol, None)

                # Attach protective Stop-Loss
                if sl and sl > 0:
                    await place_protective_stop(symbol, expected_pos_side, qty, sl, buffer)
                break

            # Check if order is still present in open orders
            status_code, data = await shark_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})
            if status_code == 200:
                orders = data if isinstance(data, list) else data.get("orders", [])
                order_cids = [o.get("clientOrderId") for o in orders]
                if entry_cid not in order_cids:
                    # Order no longer open; double check if filled
                    pos_check = await get_active_position(symbol)
                    if pos_check["side"] == expected_pos_side and pos_check["size"] > 0:
                        print(f"[FILL DETECTED] Order left book and position is active.", flush=True)
                        if sl and sl > 0:
                            await place_protective_stop(symbol, expected_pos_side, qty, sl, buffer)
                    else:
                        print(f"[ORDER CANCELLED / EXPIRED] Entry order {entry_cid} no longer in book.", flush=True)
                    active_limit_cids.pop(symbol, None)
                    break
    except asyncio.CancelledError:
        print(f"[ENTRY WATCHER] Watcher task cancelled.", flush=True)
        raise


# ==============================================================================
# WEBHOOK ENDPOINTS
# ==============================================================================
@app.get("/")
@app.head("/")
async def health_check():
    return {"status": "operational", "server_time": int(time.time() * 1000)}


@app.post("/webhook")
async def webhook_endpoint(payload: WebhookPayload):
    # Passphrase validation (if configured)
    if WEBHOOK_PASSPHRASE and payload.passphrase != WEBHOOK_PASSPHRASE:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid passphrase")

    action = payload.action.upper()
    symbol = payload.symbol.upper()
    trade_id = payload.trade_id or "0"
    now = time.time()

    # Deduplication: Discard identical repeated signals arriving within 5 seconds
    dedup_key = f"{symbol}_{action}_{trade_id}"
    if dedup_key in recent_actions and (now - recent_actions[dedup_key]) < 5.0:
        print(f"[DUPLICATE IGNORED] Discarded duplicate webhook: {dedup_key}", flush=True)
        return {"status": "ignored", "reason": "duplicate_webhook"}
    recent_actions[dedup_key] = now

    # Clean old items in deduplication cache
    for k in list(recent_actions.keys()):
        if now - recent_actions[k] > 60.0:
            del recent_actions[k]

    print(f"[WEBHOOK] Action={action} TradeID={trade_id} Qty={payload.qty} Price={payload.price} SL={payload.sl} Buffer={payload.buffer}", flush=True)

    async with action_lock:
        # ----------------------------------------------------------------------
        # ACTION: BUY / SELL (ENTRY LIMIT ORDER)
        # ----------------------------------------------------------------------
        if action in ["BUY", "SELL"]:
            # Cancel previous watcher if running
            if symbol in watcher_tasks:
                watcher_tasks[symbol].cancel()

            # Clean previous pending entry orders
            if symbol in active_limit_cids:
                await cancel_order_safe(symbol, active_limit_cids[symbol])

            cid = f"{uuid_tag()}-589624-sha"
            active_limit_cids[symbol] = cid

            order_payload = {
                "symbol": symbol,
                "side": action,
                "type": "LIMIT",
                "price": payload.price,
                "orderAmount": payload.qty,
                "clientOrderId": cid
            }

            status_code, data = await shark_request("POST", "/fapi/v1/order", order_payload)
            if status_code in [200, 201]:
                print(f"[ORDER SUCCESS] status={status_code} data={data}", flush=True)
                # Spawn persistent watcher
                watcher_tasks[symbol] = asyncio.create_task(
                    persistent_entry_watcher(
                        symbol=symbol,
                        entry_cid=cid,
                        entry_side=action,
                        qty=payload.qty,
                        sl=payload.sl,
                        buffer=payload.buffer
                    )
                )
            else:
                print(f"[ORDER REJECTED] status={status_code} data={data}", flush=True)
                active_limit_cids.pop(symbol, None)

            return {"status": "processed", "action": action, "exchange_status": status_code}

        # ----------------------------------------------------------------------
        # ACTION: CANCEL_ENTRY (SESSION EXPIRED / TIMEOUT)
        # ----------------------------------------------------------------------
        elif action == "CANCEL_ENTRY":
            if symbol in watcher_tasks:
                watcher_tasks[symbol].cancel()
                watcher_tasks.pop(symbol, None)
                print(f"[WATCHER TERMINATED] Stopped monitoring loop for {symbol}", flush=True)

            if symbol in active_limit_cids:
                entry_cid = active_limit_cids.pop(symbol)
                await cancel_order_safe(symbol, entry_cid)

            return {"status": "processed", "action": "CANCEL_ENTRY"}

        # ----------------------------------------------------------------------
        # ACTION: UPDATE_SL (TRAILING STOP / PROFIT LOCK)
        # ----------------------------------------------------------------------
        elif action == "UPDATE_SL":
            pos = await get_active_position(symbol)
            if pos["size"] == 0:
                print(f"[UPDATE_SL SKIPPED] No active position open for {symbol}.", flush=True)
                return {"status": "skipped", "reason": "no_open_position"}

            # Cancel existing SL order first
            if symbol in active_sl_cids:
                await cancel_order_safe(symbol, active_sl_cids.pop(symbol))

            new_cid = await place_protective_stop(
                symbol=symbol,
                position_side=pos["side"],
                qty=payload.qty,
                sl_price=payload.sl,
                buffer=payload.buffer
            )
            if new_cid:
                print(f"[PROFIT LOCK APPLIED] Modified Stop Limit to {payload.sl}: status=201", flush=True)

            return {"status": "processed", "action": "UPDATE_SL"}

        # ----------------------------------------------------------------------
        # ACTION: CLOSE (MARKET EXIT / TAKE PROFIT)
        # ----------------------------------------------------------------------
        elif action == "CLOSE":
            # 1. Terminate entry watcher if active
            if symbol in watcher_tasks:
                watcher_tasks[symbol].cancel()
                watcher_tasks.pop(symbol, None)

            # 2. Check current open position
            pos = await get_active_position(symbol)
            if pos["size"] == 0:
                print(f"[CLOSE SKIPPED] Position for {symbol} is already 0. Duplicate exit ignored.", flush=True)
                await cancel_all_symbol_orders(symbol)
                return {"status": "skipped", "reason": "position_already_flat"}

            # 3. Cancel resting stop-losses immediately before closing position
            if symbol in active_sl_cids:
                await cancel_order_safe(symbol, active_sl_cids.pop(symbol))
            if symbol in active_limit_cids:
                await cancel_order_safe(symbol, active_limit_cids.pop(symbol))

            # 4. Place reduceOnly Market Exit
            exit_side = "SELL" if pos["side"] == "LONG" else "BUY"
            exit_payload = {
                "symbol": symbol,
                "side": exit_side,
                "type": "MARKET",
                "orderAmount": pos["size"],
                "reduceOnly": "true"
            }

            status_code, data = await shark_request("POST", "/fapi/v1/order", exit_payload)
            if status_code in [200, 201]:
                print(f"[MARKET EXIT] Closed {pos['side']} position of {pos['size']} {symbol}", flush=True)
            else:
                print(f"[MARKET EXIT ERROR] status={status_code} data={data}", flush=True)

            return {"status": "processed", "action": "CLOSE", "exchange_status": status_code}

        else:
            return {"status": "unrecognized_action", "action": action}


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
