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

def allowed_gai_family():
    return socket.AF_INET

urllib3_cn.allowed_gai_family = allowed_gai_family

app = FastAPI()

SHARK_BASE_URL = "https://api.sharkexchange.in"
SHARK_API_KEY = os.getenv("SHARK_API_KEY", "").strip()
SHARK_API_SECRET = os.getenv("SHARK_API_SECRET", "").strip()
WEBHOOK_PASSPHRASE = os.getenv("WEBHOOK_PASSPHRASE", "MY_SECRET_KEY").strip()

CURRENT_TRADE_ID = None
CURRENT_POSITION_SIDE = None
ACTIVE_SL_CLIENT_IDS = []
ACTIVE_ENTRY_ORDER_ID = None


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
                if pos.get("symbol", "").upper() == clean_symbol:
                    raw_qty = float(pos.get("positionAmount", 0.0) or pos.get("amount", 0.0) or 0.0)
                    side = str(pos.get("side", "") or pos.get("positionSide", "")).upper()
                    if side in ["SELL", "SHORT"]:
                        return -abs(raw_qty)
                    elif side in ["BUY", "LONG"]:
                        return abs(raw_qty)
                    return raw_qty
        return 0.0
    except Exception as e:
        print(f"[FETCH ERROR]: {e}")
        return 0.0


def delete_single_order(client_order_id: str) -> bool:
    try:
        ts = str(int(time.time() * 1000))
        payload = {"timestamp": ts, "clientOrderId": str(client_order_id)}
        body = json.dumps(payload, separators=(",", ":"))
        headers = get_headers(body)

        resp = requests.delete(f"{SHARK_BASE_URL}/v1/order/delete-order", data=body, headers=headers, timeout=5)
        print(f"[CLEANUP] Deleted Order ({client_order_id}) -> HTTP {resp.status_code}")
        return resp.status_code in [200, 201, 204]
    except Exception as e:
        print(f"[CLEANUP ERROR]: {e}")
        return False


def cancel_all_tracked_stops():
    global ACTIVE_SL_CLIENT_IDS
    if not ACTIVE_SL_CLIENT_IDS:
        return
    for cid in list(ACTIVE_SL_CLIENT_IDS):
        delete_single_order(cid)
    ACTIVE_SL_CLIENT_IDS = []


def cancel_pending_limit_entry():
    global ACTIVE_ENTRY_ORDER_ID
    if ACTIVE_ENTRY_ORDER_ID:
        print(f"[ENTRY CLEANUP] Canceling unfilled limit entry: {ACTIVE_ENTRY_ORDER_ID}")
        delete_single_order(ACTIVE_ENTRY_ORDER_ID)
        ACTIVE_ENTRY_ORDER_ID = None


def place_stop_loss(symbol: str, side: str, quantity: float, stop_price: float, ref_price: float = 0.0):
    global ACTIVE_SL_CLIENT_IDS
    try:
        if ref_price > 0:
            if side == "SELL" and stop_price >= ref_price:
                print(f"[GUARD] Long SL {stop_price} >= Price {ref_price}. Skipping.")
                return
            if side == "BUY" and stop_price <= ref_price:
                print(f"[GUARD] Short SL {stop_price} <= Price {ref_price}. Skipping.")
                return

        time.sleep(0.15)
        offset = 15.0
        limit_price = round(stop_price - offset, 2) if side == "SELL" else round(stop_price + offset, 2)

        sl_params = {
            "timestamp": str(int(time.time() * 1000)),
            "placeType": "ORDER_FORM",
            "quantity": quantity,
            "side": side,
            "symbol": symbol,
            "type": "STOP_LIMIT",
            "price": limit_price,
            "stopPrice": stop_price,
            "reduceOnly": True,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL"
        }

        sl_body = json.dumps(sl_params, separators=(",", ":"))
        sl_headers = get_headers(sl_body)

        resp = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=sl_body, headers=sl_headers, timeout=5)
        if resp.status_code in [200, 201]:
            res_data = resp.json()
            cid = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
            if cid:
                ACTIVE_SL_CLIENT_IDS.append(cid)
                print(f">>> [SUCCESS] STOP_LIMIT Placed at {stop_price} (Limit: {limit_price}) | ID: {cid}")
        else:
            print(f">>> [SL ERROR HTTP {resp.status_code}]: {resp.text}")
    except Exception as e:
        print(f"[SL EXEC ERROR]: {e}")


def wait_for_fill_and_set_sl(clean_symbol: str, target_side: str, target_qty: float, entry_price: float, trade_id: int):
    """
    Background worker: monitors Shark Exchange until the Limit Entry fills,
    then automatically places the exact +/- 100 points initial SL.
    """
    global CURRENT_TRADE_ID, CURRENT_POSITION_SIDE
    is_buy = target_side == "BUY"
    expected_pos = target_qty if is_buy else -target_qty

    print(f"[WATCHER] Polling exchange for fill on Limit {target_side} {target_qty} @ {entry_price}...")

    # Poll up to 60 times (every 2 seconds = 2 minutes active monitoring)
    for _ in range(60):
        time.sleep(2.0)
        if CURRENT_TRADE_ID != trade_id:
            print(f"[WATCHER] Trade {trade_id} superseded by a newer trade. Exiting watcher.")
            return

        current_pos = get_real_exchange_position(clean_symbol)

        # Order has filled
        if (is_buy and current_pos >= (expected_pos - 0.0001)) or (not is_buy and current_pos <= (expected_pos + 0.0001)):
            print(f">>> [LIMIT FILLED] Active position confirmed at {current_pos}. Placing initial Stop-Limit...")
            CURRENT_POSITION_SIDE = target_side
            
            # Compute exact 100 points from limit fill
            if is_buy:
                initial_stop = round(entry_price - 100.0, 2)
                sl_side = "SELL"
            else:
                initial_stop = round(entry_price + 100.0, 2)
                sl_side = "BUY"

            place_stop_loss(clean_symbol, sl_side, target_qty, initial_stop, entry_price)
            return

    print(f"[WATCHER] Limit order did not fill within timeout window.")


def execute_entry_order(action: str, symbol: str, quantity: float, target_limit_price: float, trade_id: int):
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID, ACTIVE_ENTRY_ORDER_ID
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()
        target_qty = float(quantity)

        # Step 1: Clean up any past stops and unfilled limits
        cancel_pending_limit_entry()
        cancel_all_tracked_stops()

        real_current_pos = get_real_exchange_position(clean_symbol)
        is_buy_intent = "BUY" in action.upper()
        intended_net_pos = target_qty if is_buy_intent else -target_qty

        diff = round(intended_net_pos - real_current_pos, 4)
        if abs(diff) < 0.0001:
            print(f"[NO-OP] Position already matches intended {intended_net_pos}.")
            CURRENT_TRADE_ID = trade_id
            CURRENT_POSITION_SIDE = "BUY" if is_buy_intent else "SELL"
            return

        side = "BUY" if diff > 0 else "SELL"
        exec_qty = round(abs(diff), 4)
        CURRENT_TRADE_ID = trade_id

        # Step 2: Submit LIMIT Entry Order to Shark Exchange
        entry_params = {
            "timestamp": str(int(time.time() * 1000)),
            "placeType": "ORDER_FORM",
            "quantity": exec_qty,
            "side": side,
            "symbol": clean_symbol,
            "type": "LIMIT",
            "price": round(target_limit_price, 2),
            "reduceOnly": False,
            "marginAsset": "INR",
            "deviceType": "WEB",
            "userCategory": "EXTERNAL"
        }

        entry_body = json.dumps(entry_params, separators=(",", ":"))
        entry_headers = get_headers(entry_body)

        print(f"\n[LIMIT ENTRY] Placing {side} {exec_qty} {clean_symbol} @ Limit Price {target_limit_price}...")
        resp_entry = requests.post(f"{SHARK_BASE_URL}/v1/order/place-order", data=entry_body, headers=entry_headers, timeout=5)

        if resp_entry.status_code in [200, 201]:
            res_data = resp_entry.json()
            ACTIVE_ENTRY_ORDER_ID = res_data.get("clientOrderId") or res_data.get("data", {}).get("clientOrderId")
            print(f">>> [SUCCESS] Limit Entry Posted to Book. Order ID: {ACTIVE_ENTRY_ORDER_ID}")

            # Step 3: Spawn thread to wait for fill, then place initial 100 pt SL
            threading.Thread(
                target=wait_for_fill_and_set_sl,
                args=(clean_symbol, side, target_qty, target_limit_price, trade_id),
                daemon=True
            ).start()
        else:
            print(f">>> [LIMIT ENTRY ERROR HTTP {resp_entry.status_code}]: {resp_entry.text}")

    except Exception as e:
        print(f"[LIMIT ENTRY EXEC ERROR]: {e}")


def update_trailing_stop(symbol: str, quantity: float, sl_price: float, current_price: float, trade_id: int):
    global CURRENT_POSITION_SIDE, CURRENT_TRADE_ID
    try:
        clean_symbol = symbol.replace(".P", "").replace(".p", "").replace("-", "").replace("/", "").upper()

        real_pos = get_real_exchange_position(clean_symbol)
        if abs(real_pos) < 0.0001:
            print(f"[REJECTED UPDATE_SL] No open position on Shark Exchange ({real_pos}). Discarding trail.")
            return

        if CURRENT_TRADE_ID is not None and trade_id != CURRENT_TRADE_ID:
            print(f"[DESYNC GUARD] Alert trade_id ({trade_id}) != bot trade_id ({CURRENT_TRADE_ID}). Discarded.")
            return

        pos_side = "BUY" if real_pos > 0 else "SELL"
        order_qty = round(abs(real_pos), 4)
        stop_price = round(float(sl_price), 2)
        ref_price = float(current_price) if current_price else 0.0

        if stop_price > 0:
            sl_side = "SELL" if pos_side == "BUY" else "BUY"
            cancel_all_tracked_stops()
            place_stop_loss(clean_symbol, sl_side, order_qty, stop_price, ref_price)

    except Exception as e:
        print(f"[TRAILING SL ERROR]: {e}")


@app.api_route("/", methods=["GET", "HEAD"])
def home():
    return {"status": "awake", "service": "Shark Limit-Trading Bot"}


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
    quantity = float(data.get("quantity", 0.05))
    target_limit_price = float(data.get("price", 0.0))
    sl_price = float(data.get("sl_price", 0.0))
    trade_id = int(data.get("trade_id", 0))

    print(f"\n[ALERT RECEIVED] Action: {action} | Limit Price: {target_limit_price} | TradeID: {trade_id}")

    if action in ["BUY", "SELL"]:
        threading.Thread(
            target=execute_entry_order,
            args=(action, symbol, quantity, target_limit_price, trade_id),
            daemon=True
        ).start()

    elif action == "UPDATE_SL":
        threading.Thread(
            target=update_trailing_stop,
            args=(symbol, quantity, sl_price, target_limit_price, trade_id),
            daemon=True
        ).start()

    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 5000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)
