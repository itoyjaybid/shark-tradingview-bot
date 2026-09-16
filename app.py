from fastapi import FastAPI, Request, HTTPException
import os
import time
import json
import hmac
import hashlib
import socket
import requests
import threading

# Force IPv4 for outbound requests on cloud hosts (Render/Railway)
import urllib3.util.connection as urllib3_cn

def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

app = FastAPI()

# ==========================================
# SHARK EXCHANGE CONFIGURATION
# ==========================================
SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip()
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip()

CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []

# 15-point threshold limit buffer
SL_LIMIT_BUFFER_PTS = 15.0


def format_price(val: float):
    """Formats prices to avoid .0 float signature mismatches on Node.js backends."""
    f = round(float(val), 2)
    return int(f) if f.is_integer() else f


def format_qty(val: float):
    """Formats quantities to avoid .0 float signature mismatches on Node.js backends."""
    f = round(float(val), 4)
    return int(f) if f.is_integer() else f


def generate_signature(secret: str, data: str) -> str:
    return hmac.new(secret.encode("utf-8"), data.encode("utf-8"), hashlib.sha256).hexdigest()


def get_headers(payload_str: str) -> dict:
    return {
        "api-key": SHARK_API_KEY,
        "signature": generate_signature(SHARK_API_SECRET, payload_str),
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }


def get_real_exchange_position(symbol: str) -> float:
    """
    Fetches the live open position size directly from Shark Exchange.
    Returns:
       > 0 for Long (e.g., +0.002)
       < 0 for Short (e.g., -0.002)
       0.0 for Flat
    """
    try:
        ts = str(int(time.time() * 1000))
        payload = {"timestamp": ts}
        body = json.dumps(payload, separators=(",", ":"))
        headers = get_headers(body)

        resp = requests.post(f"{SHARK_BASE_URL}/v1/position/all-positions", data=body, headers=headers, timeout=5)
        if resp.status_code == 200:
            res_json = resp.json()
            positions = res_json.get("data", []) if isinstance(res_json, dict) else res_json

            clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

            for pos in positions:
                pos_sym = pos.get("symbol", "").upper()
                if pos_sym == clean_symbol:
                    raw_qty = float(pos.get("positionAmount", 0.0) or pos.get("amount", 0.0) or 0.0)
                    side = str(pos.get("side", "") or pos.get("positionSide", "")).upper()
                    
                    if side in ["SELL", "SHORT"]:
                        return -abs(raw_qty)
                    elif side in ["BUY", "LONG"]:
                        return abs(raw_qty)
                    return raw_qty

        return 0.0
    except Exception as e:
        print(f"[FETCH POSITION ERROR]: {e}")
        return 0.0


def delete_single_order(client_order_id: str) -> bool:
    """Deletes an order using the payload schema required by Shark Exchange."""
    try:
        ts = str(int(time.time() * 1000))
        payload = {
            "timestamp": ts,
            "clientOrderId": str(client_order_id)
        }
        body = json.dumps(payload, separators=(",", ":"))
        headers = get_headers(body)

        resp = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=5)
        print(f"[CLEANUP] Deleted SL ({client_order_id}) -> HTTP {resp.status_code}: {resp.text}")
        return resp.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}")
        return False


def cancel_all_tracked_stops():
    """Iterates through and cancels all active resting Stop Loss orders."""
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return

    print(f"[CLEANUP] Deleting active SL clientOrderIds: {ACTIVE_SL_CLIENT_IDS}")
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid)
    ACTIVE_SL_CLIENT_IDS = []


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0):
    """Places STOP_LIMIT order with 15-point buffer, reduceOnly=True, and signature-safe types."""
    global ACTIVE_SL_CLIENT_IDS
    try:
        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[REJECTED GUARD] Long SL ({stop_price}) >= Market Price ({ref_price}). Skipping.")
                return
            if side == "BUY" and stop_price <= ref_price:
                print(f"[REJECTED GUARD] Short SL ({stop_price}) <= Market Price ({ref_price}). Skipping.")
                return

        time.sleep(0.2)
        sl_timestamp = str(int(time.time() * 1000))

        # Calculate limit price with 15-point buffer
        if side == "SELL":
            limit_val = float(stop_price) - SL_LIMIT_BUFFER_PTS
        else:
            limit_val = float(stop_price) + SL_LIMIT_BUFFER_PTS

        clean_stop = format_price(stop_price)
        clean_limit = format_price(limit_val)
        clean_quantity = format_qty(quantity)

        sl_params = {
            "timestamp": sl_timestamp,
            "placeType": "ORDER_FORM",
            "quantity": clean_quantity,
            "side": side,
            "symbol": symbol,
            "type": "STOP_LIMIT",
            "reduceOnly": True,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL",
            "stopPrice": clean_stop,
            "price": clean_limit
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"))
        sl_headers = get_headers(sl_body)

        print(f"[STOP LIMIT] Submitting {side} for {clean_quantity} {symbol} | Trigger: {clean_stop} | Limit: {clean_limit}...")
        resp = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=sl_body, headers=sl_headers, timeout=5)

        print(f"[SL RESPONSE] HTTP {resp.status_code}: {resp.text}")

        if resp.status_code in [200, 201]:
            res_data = resp.json()
            cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
            if cid:
                ACTIVE_SL_CLIENT_IDS.append(cid)
                print(f">>> [SUCCESS] SL Placed! Active clientOrderId: {cid}")
            else:
                print(f">>> [SUCCESS] Response: {res_data}")
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
    except Exception as e:
        print(f"[SL EXECUTION ERROR]: {e}")


def execute_entry_order(action: str, symbol: str, quantity: float, sl_price: float = 0.0, current_price: float = 0.0):
    """
    Executes entry dynamically based on the live position size on Shark Exchange.
    Automatically prevents over-sizing from duplicate or reversed alerts.
    """
    global CURRENT_POSITION_SIDE
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
        target_qty = format_qty(quantity)

        # 1. Query live balance on the exchange
        real_current_pos = get_real_exchange_position(clean_symbol)
        print(f"[EXCHANGE AUDIT] Symbol: {clean_symbol} | Real Live Position: {real_current_pos}")

        # 2. Compute intended position
        is_buy_intent = "BUY" in action.upper()
        intended_net_pos = target_qty if is_buy_intent else -target_qty

        # 3. Calculate exact delta to fill
        diff = round(intended_net_pos - real_current_pos, 4)

        if abs(diff) < 0.0001:
            print(f"[NO-OP] Live position ({real_current_pos}) already matches target ({intended_net_pos}).")
            return

        side = "BUY" if diff > 0 else "SELL"
        exec_qty = format_qty(abs(diff))

        stop_price = format_price(sl_price) if sl_price else 0.0
        ref_price = format_price(current_price) if current_price else 0.0

        # Step A: Clear existing resting stop orders
        cancel_all_tracked_stops()

        # Step B: Submit Market Entry Order
        timestamp = str(int(time.time() * 1000))
        entry_params = {
            "timestamp": timestamp,
            "placeType": "ORDER_FORM",
            "quantity": exec_qty,
            "side": side,
            "symbol": clean_symbol,
            "type": "MARKET",
            "reduceOnly": False,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL"
        }

        entry_body = json.dumps(entry_params, separators=(",", ":"))
        entry_headers = get_headers(entry_body)

        print(f"\n[ENTRY] Real Pos: {real_current_pos} -> Executing {side} {exec_qty} {clean_symbol} (Target Net: {target_qty})...")
        resp_entry = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=entry_body, headers=entry_headers, timeout=5)

        if resp_entry.status_code in [200, 201]:
            CURRENT_POSITION_SIDE = "BUY" if is_buy_intent else "SELL"
            print(f">>> [SUCCESS] Entry Filled! Response: {resp_entry.json()}")
        else:
            print(f">>> [ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")
            return

        # Step C: Place Stop-Limit sized strictly to target net position
        if stop_price > 0:
            sl_side = "SELL" if is_buy_intent else "BUY"
            place_stop_loss(clean_symbol, sl_side, target_qty, stop_price, ref_price)

    except Exception as e:
        print(f"[BRIDGE EXECUTION ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float):
    """Safely updates trailing stop loss level with 15-point limit offset."""
    global CURRENT_POSITION_SIDE
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

        if CURRENT_POSITION_SIDE is None:
            real_pos = get_real_exchange_position(clean_symbol)
            if real_pos > 0:
                CURRENT_POSITION_SIDE = "BUY"
            elif real_pos < 0:
                CURRENT_POSITION_SIDE = "SELL"
            else:
                print(f"[GUARD TRIGGERED] Discarding UPDATE_SL: No active trade tracked for {clean_symbol}.")
                return

        order_qty = format_qty(quantity)
        stop_price = format_price(sl_price) if sl_price else 0.0
        ref_price = format_price(current_price) if current_price else 0.0

        if stop_price > 0:
            sl_side = "SELL" if CURRENT_POSITION_SIDE == "BUY" else "BUY"
            cancel_all_tracked_stops()
            place_stop_loss(clean_symbol, sl_side, order_qty, stop_price, ref_price)

    except Exception as e:
        print(f"[TRAILING SL ERROR]: {e}")


def reset_and_cleanup():
    """Resets tracking state and clears any leftover resting stops."""
    global CURRENT_POSITION_SIDE
    CURRENT_POSITION_SIDE = None
    cancel_all_tracked_stops()


# ==========================================
# FASTAPI ROUTES
# ==========================================
@app.api_route("/", methods=["GET", "HEAD"])
def home():
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
    sl_price = float(data.get("sl_price", 0.0))
    current_price = float(data.get("price", 0.0))

    print(f"\n[ALERT RECEIVED] Action: {action} | Symbol: {symbol} | Qty: {quantity} | SL: {sl_price} | Price: {current_price}")

    if action in ["BUY", "SELL", "REVERSE_BUY", "REVERSE_SELL"]:
        threading.Thread(
            target=execute_entry_order,
            args=(action, symbol, quantity, sl_price, current_price),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=update_trailing_stop,
            args=(symbol, quantity, sl_price, current_price),
            daemon=True
        ).start()

    elif action in ["SL_EXIT", "EXIT", "CLOSE"]:
        threading.Thread(
            target=reset_and_cleanup,
            daemon=True
        ).start()

    else:
        return {"status": "ignored", "reason": f"Unknown action: {action}"}

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
