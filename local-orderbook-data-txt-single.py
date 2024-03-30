import json
import time
import asyncio
from datetime import datetime, timezone
from httpx import AsyncClient
from websockets import connect
import aiofiles
import os

# cmd - "tail -f <filename>" to check saving works correctly

async def orderbook_download_txt(
        path: str, 
        pair: str
):
    pair_lower = pair.lower()
    websocket_url = f"wss://stream.binance.com:9443/ws/{pair_lower}@depth@100ms"
    rest_url = f"https://api.binance.com/api/v3/depth"

    folder_path = f"{path}/{pair}/"
    os.makedirs(folder_path, exist_ok=True)
    # Path storing depth updates
    path_u = f"{folder_path}depth-updates/"
    os.makedirs(path_u, exist_ok=True)
    # Path storing snapshots
    path_s = f"{folder_path}snapshots/"
    os.makedirs(path_s, exist_ok=True)

    params = {
        "symbol": pair.upper(),
        "limit": 1000
    }

    update_counter = 0

    max_counter = 6000
    
    async with connect(websocket_url) as websocket:
        while True:
            today = datetime.now(timezone.utc).date()
            data = await websocket.recv()
            async with aiofiles.open(f"{path_u}{pair_lower}-depth-updates-{today}.txt", mode="a") as f:
                await f.write(data + '\n')

            if update_counter % max_counter == 0:
                await fetch_snapshot(path_s=path_s, 
                                     pair_lower=pair_lower, 
                                     rest_url=rest_url, 
                                     params=params)
                update_counter = 0
            update_counter += 1

async def fetch_snapshot(
        path_s: str,
        pair_lower: str,
        rest_url: str,
        params: dict
):
    async with AsyncClient() as client:
        snapshot_response = await client.get(rest_url, params=params)
        snapshot = snapshot_response.json()
        snapshot["time"] = datetime.now(timezone.utc).isoformat(sep=' ', timespec='microseconds')
    
    today = datetime.now(timezone.utc).date()

    async with aiofiles.open(f'{path_s}{pair_lower}-snapshots-{today}.txt', mode='a') as f:
        await f.write(json.dumps(snapshot) + '\n')

# Run async corountine
# Save as txt file on daily basis - snapshots within intervals, and order streams
asyncio.run(orderbook_download_txt(path="local-orderbook-data", pair="DOGEUSDT"))
