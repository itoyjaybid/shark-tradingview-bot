SERVER_TIME_OFFSET_MS = 0

def sync_exchange_time():
    """Calculates clock drift between Render and Shark Exchange."""
    global SERVER_TIME_OFFSET_MS
    try:
        resp = requests.get(f"{SHARK_BASE_URL}/v1/time", timeout=2)
        if resp.status_code == 200:
            server_ts = int(resp.json().get("serverTime") or resp.json().get("data", 0))
            if server_ts > 0:
                local_ts = int(time.time() * 1000)
                SERVER_TIME_OFFSET_MS = server_ts - local_ts
                print(f"[TIME SYNC] Synced clock offset: {SERVER_TIME_OFFSET_MS}ms")
    except Exception as e:
        print(f"[TIME SYNC WARN]: {e}")

def get_synced_timestamp() -> str:
    """Returns local time adjusted by the exchange server drift."""
    return str(int(time.time() * 1000) + SERVER_TIME_OFFSET_MS)

@app.on_event("startup")
def on_startup():
    sync_exchange_time()
