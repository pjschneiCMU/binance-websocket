"""
Binance - Orderbook (bid price, bid volume, ask price, ask volume, timestamp)
- Modify script to receive orderbook in more depth
- Change 'counter' and 'interval' based on storage requirements
"""

import websocket
import json
import csv
import os
import threading
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)

# Global variables for WebSocket connection and CSV writer
ws = None
csv_writer = None
csv_file = None
save_counter = -1

# Constants - Adopt accordingly
depth = 10
symbol = 'BTCUSDT'
interval = 10

def on_message(ws, message):
    global csv_writer
    message = json.loads(message)
    if 'lastUpdateId' in message:
        timestamp = datetime.now().isoformat(sep=' ', timespec='microseconds') 
        # Concatenate bid and ask levels as a single row
        row = [message['lastUpdateId'], timestamp]
        for i in range(0, depth):
            bid_price, bid_volume = message['bids'][i]
            ask_price, ask_volume = message['asks'][i]
            row.extend([float(bid_price), float(bid_volume), float(ask_price), float(ask_volume)])
        csv_writer.writerow(row)

# Function to handle WebSocket errors
def on_error(ws, error):
     logging.error(error)

# Function to open the WebSocket connection and subscribe to the order book channel
def on_open_factory(symbol, depth):
    def on_open(ws):
        logging.info("WebSocket connection opened")
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [
                f"{symbol.lower()}@depth{depth}@100ms",
            ],
            "id": 1
        }))
    return on_open

# Function to periodically save the CSV file
def save_csv_periodically(csv_file_path):
    global csv_writer, save_counter
    threading.Timer(interval, save_csv_periodically, args=(csv_file_path,)).start()

    # Increment the save counter
    save_counter += 1

    # Check if it's time to create a new CSV file
    if save_counter % 60 == 0:
        csv_file_path = create_csv_file()

# Function to create a new CSV file
def create_csv_file():
    # Create a new folder for each new calendar day
    current_date = datetime.now().strftime("%Y%m%d")
    folder_path = f"data/{symbol}/{current_date}"
    os.makedirs(folder_path, exist_ok=True)

    # Create a new CSV file with the current timestamp
    current_time = datetime.now()
    csv_filename = f'{folder_path}/bid_ask_depth_{depth}_data_{symbol}_{current_time.strftime("%Y%m%d_%H%M%S")}.csv'

    global csv_writer, csv_file
    if csv_file is not None:
        csv_file.close()
    csv_file = open_csv_file(csv_filename)
    csv_writer = csv.writer(csv_file)

    # Write the header if the file is empty
    if csv_file.tell() == 0:
        header = ['Update ID', 'Timestamp']
        for i in range(1, depth+1):
            header.extend([f'Bid Price {i}', f'Bid Volume {i}', f'Ask Price {i}', f'Ask Volume {i}'])
        csv_writer.writerow(header)

    return csv_filename

def open_csv_file(csv_filename):
    return open(csv_filename, mode='w', newline='', encoding='utf-8')

def websocket_thread():
    global ws
    while True:
        ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        on_open = on_open_factory(symbol, depth)
        ws.on_open = on_open
        ws.run_forever()

# Function to handle WebSocket connection closure
def on_close(ws):
    global csv_writer, csv_file
    logging.info("WebSocket connection closed")
    csv_writer = None
    if csv_file is not None:
        csv_file.close()

if __name__ == "__main__":
    # Create the initial CSV file
    csv_file_path = create_csv_file()

    # Start the WebSocket thread in a separate thread
    ws_thread = threading.Thread(target=websocket_thread)
    ws_thread.start()

    # Periodically save the CSV file every 10 seconds
    save_csv_periodically(csv_file_path)
