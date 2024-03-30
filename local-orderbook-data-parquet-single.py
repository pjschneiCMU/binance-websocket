from datetime import datetime, timezone
import asyncio
import json
from websockets import connect
from httpx import AsyncClient
import pyarrow as pa
import pyarrow.parquet as pq
import os

async def orderbook_download_parquet(
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

    # Create an empty list to store data
    data_list = []

    # Context manager handles subscribing, disconnecting etc.
    async with connect(websocket_url) as websocket:
        while True:
            data = await websocket.recv()
            # Append data to the list
            data_list.append(data)

            # Write data to Parquet file periodically or when a certain condition is met
            if len(data_list) >= 6000:  # For example, write every 6000 data points - every 10 minutes
                today = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                await write_to_parquet(path_u=path_u, 
                                       pair_lower=pair_lower, 
                                       today=today, 
                                       data_list=data_list)
                data_list = []  # Clear the list after writing to the file
                await fetch_snapshot(path_s=path_s, 
                                     pair_lower=pair_lower, 
                                     rest_url=rest_url, 
                                     params=params, 
                                     today=today)

async def write_to_parquet(
        path_u: str,
        pair_lower: str, 
        today: datetime, 
        data_list: list
):
    
    # Create a PyArrow schema based on the data structure
    schema = pa.schema([
        ('e', pa.string()),  # Event type
        ('E', pa.int64()),   # Event time
        ('s', pa.string()),  # Symbol
        ('U', pa.int64()),   # Update ID of the first event in the update
        ('u', pa.int64()),   # Update ID of the last event in the update
        ('b', pa.list_(pa.list_(pa.string()))),  # Bids (list of lists of strings)
        ('a', pa.list_(pa.list_(pa.string())))   # Asks (list of lists of strings)
    ])

    # Convert the list of data to a dictionary with keys corresponding to schema fields
    data_dict = {
        'e': [],
        'E': [],
        's': [],
        'U': [],
        'u': [],
        'b': [],
        'a': []
    }

    for item in data_list:
        parsed_data = json.loads(item)
        data_dict['e'].append(parsed_data['e'])
        data_dict['E'].append(parsed_data['E'])
        data_dict['s'].append(parsed_data['s'])
        data_dict['U'].append(parsed_data['U'])
        data_dict['u'].append(parsed_data['u'])
        data_dict['b'].append(parsed_data['b'])
        data_dict['a'].append(parsed_data['a'])

    # Convert the dictionary to a PyArrow table
    table = pa.Table.from_pydict(data_dict, schema=schema)
    
    # Write the table to a Parquet file
    file_path = f"{path_u}{pair_lower}-depth-updates-{today}.parquet"
    pq.write_table(table, file_path, compression='snappy')

async def fetch_snapshot(
        path_s: str,
        pair_lower: str, 
        rest_url: str, 
        params: dict, 
        today: datetime
):
    
    async with AsyncClient() as client:
        snapshot_response = await client.get(rest_url, params=params)
        snapshot = snapshot_response.json()
        snapshot["time"] = datetime.now(timezone.utc).isoformat(sep=' ', timespec='microseconds')
    
    schema = pa.schema([
        ('lastUpdateId', pa.int64()),  # Event type
        ('bids', pa.list_(pa.list_(pa.string()))),  # Bids (list of lists of strings)
        ('asks', pa.list_(pa.list_(pa.string()))),   # Asks (list of lists of strings)
        ('time', pa.string())  # Time
    ])
    # Convert the dictionary to a PyArrow table
    snapshot['lastUpdateId'] = [snapshot['lastUpdateId']]
    snapshot['time'] = [snapshot['time']]
    snapshot['bids'] = [snapshot['bids']]
    snapshot['asks'] = [snapshot['asks']]

    table = pa.Table.from_pydict(snapshot, schema=schema)
    
    # Write the table to a Parquet file
    file_path = f"{path_s}{pair_lower}-snapshot-{today}.parquet"
    pq.write_table(table, file_path, compression='snappy')
    
# # Run async corountine
# Save depth streams and orderbook snapshots to reconstruct orderbook
asyncio.run(orderbook_download_parquet(path="local-orderbook-data", pair="DOGEUSDT"))