# 1. Clean prior pending entry and stop-loss orders
cancel_pending_limit_entry()
cancel_all_tracked_stops()

# 2. Condition 7 & 13: Close previous position before reversing
close_position_immediately(clean_symbol)
cancel_all_tracked_stops()

is_buy_intent = "BUY" in action.upper()
side = "BUY" if is_buy_intent else "SELL"
CURRENT_TRADE_ID = trade_id

ts = str(int(time.time() * 1000))

entry_params = {
    "deviceType": "WEB",
    "marginAsset": "INR",
    "placeType": "ORDER_FORM",
    "price": clean_price,
    "quantity": clean_qty,
    "reduceOnly": False,
    "side": side,
    "symbol": clean_symbol,
    "timestamp": ts,
    "type": "LIMIT",
    "userCategory": "EXTERNAL"
}

entry_body = json.dumps(entry_params, separators=(",", ":"), sort_keys=True)
entry_headers = get_headers(entry_body)

resp_entry = requests.post(
    f"{SHARK_BASE_URL}/v1/order/place-order",
    data=entry_body.encode("utf-8"),
    headers=entry_headers,
    timeout=3
)
