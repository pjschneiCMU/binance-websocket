"""
Binance - bookTicker (bid price, bid volume, ask price, ask volume) - real-time data
- Change 'counter' and 'interval' based on storage requirements
"""

import asyncio
import json
import csv
import os
from datetime import datetime, timezone

import websockets

# Global variables for WebSocket connection and CSV writer
csv_writer = None
csv_file = None
save_counter = 0

async def on_message(message):
    global csv_writer
    message = json.loads(message)
    if 's' in message and message['s'] == symbol:
        bid_price = float(message['b'])
        bid_volume = float(message['B'])
        ask_price = float(message['a'])
        ask_volume = float(message['A'])
        timestamp = datetime.now(timezone.utc).isoformat(sep=' ', timespec='microseconds')
        csv_writer.writerow([timestamp, bid_price, bid_volume, ask_price, ask_volume])

# Function to handle WebSocket errors
def on_error(error):
    print(error)

# Function to open the WebSocket connection and subscribe to the order book channel
async def on_open_factory(symbol):
    print("WebSocket connection opened")
    await ws.send(json.dumps({
        "method": "SUBSCRIBE",
        "params": [
            f"{symbol.lower()}@bookTicker",
        ],
        "id": 1
    }))

# Function to periodically save the CSV file
async def save_csv_periodically(csv_file_path, symbol, interval):
    global csv_writer, save_counter
    while True:
        await asyncio.sleep(interval)

        # Increment the save counter
        save_counter += 1

        # Check if it's time to create a new CSV file - save new file every 10 mins
        if save_counter % 60 == 0:
            csv_file_path = create_csv_file(symbol)


# Function to create a new CSV file
def create_csv_file(symbol):
    # Create a new folder for each new calendar day
    current_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    folder_path = f"data/{symbol}/{current_date}"
    os.makedirs(folder_path, exist_ok=True)

    # Create a new CSV file with the current timestamp
    current_time = datetime.now(timezone.utc)
    csv_filename = f'{folder_path}/bid_ask_data_{symbol}_{current_time.strftime("%Y%m%d_%H%M%S")}.csv'

    global csv_writer, csv_file
    if csv_file is not None:
        csv_file.close()
    csv_file = open_csv_file(csv_filename)
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(['Timestamp', 'Bid Price', 'Bid Volume', 'Ask Price', 'Ask Volume'])

    return csv_filename

def open_csv_file(csv_filename):
    return open(csv_filename, mode='w', newline='', encoding='utf-8')

async def websocket_thread():
    global ws
    async with websockets.connect("wss://stream.binance.com:9443/ws") as ws:
        await on_open_factory(symbol)
        async for message in ws:
            await on_message(message)

# Function to handle WebSocket connection closure
def on_close():
    global csv_writer, csv_file
    print("WebSocket connection closed")
    csv_writer = None
    if csv_file is not None:
        csv_file.close()

if __name__ == "__main__":
    symbol = 'BTCUSDT'
    # Create the initial CSV file
    csv_file_path = create_csv_file(symbol)

    # Create an event loop
    loop = asyncio.get_event_loop()

    # Start the WebSocket thread in the event loop
    websocket_task = loop.create_task(websocket_thread())

    # Schedule the save_csv_periodically function to be called periodically
    save_interval = 10
    loop.create_task(save_csv_periodically(csv_file_path, symbol, save_interval))

    try:
        # Run the event loop
        loop.run_until_complete(websocket_task)
    except KeyboardInterrupt:
        pass
    finally:
        # Close the event loop
        loop.close()
