"""
Binance - bookTicker (bid price, bid volume, ask price, ask volume) - real-time data
- Change 'counter' and 'interval' based on storage requirements
"""

import websocket
import json
import csv
import os
import threading
from datetime import datetime, timedelta

# Global variables for WebSocket connection and CSV writer
ws = None
csv_writer = None
csv_file = None
save_counter = -1

def on_message(ws, message):
    global csv_writer
    message = json.loads(message)
    if 's' in message and message['s'] == symbol:
        bid_price = float(message['b'])
        bid_volume = float(message['B'])
        ask_price = float(message['a'])
        ask_volume = float(message['A'])
        timestamp = datetime.utcnow().isoformat(sep=' ', timespec='microseconds')
        csv_writer.writerow([timestamp, bid_price, bid_volume, ask_price, ask_volume])

# Function to handle WebSocket errors
def on_error(ws, error):
    print(error)

# Function to open the WebSocket connection and subscribe to the order book channel
def on_open_factory(symbol):
    def on_open(ws):
        print("WebSocket connection opened")
        ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": [
                f"{symbol.lower()}@bookTicker",
            ],
            "id": 1
        }))
    return on_open

# Function to periodically save the CSV file
def save_csv_periodically(csv_file_path, symbol, interval):
    global csv_writer, save_counter
    threading.Timer(interval, save_csv_periodically, args=(csv_file_path, symbol, interval)).start()

    # Increment the save counter
    save_counter += 1

    # Check if it's time to create a new CSV file
    if save_counter % 60 == 0:
        csv_file_path = create_csv_file(symbol)

    # You can perform additional logic here if needed before saving the CSV

# Function to create a new CSV file
def create_csv_file(symbol):
    # Create a new folder for each new calendar day
    current_date = datetime.now().strftime("%Y%m%d")
    folder_path = f"data/{symbol}/{current_date}"
    os.makedirs(folder_path, exist_ok=True)

    # Create a new CSV file with the current timestamp
    current_time = datetime.now()
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

def websocket_thread():
    global ws
    while True:
        ws = websocket.WebSocketApp(
            "wss://stream.binance.com:9443/ws",
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        on_open = on_open_factory(symbol)
        ws.on_open = on_open
        ws.run_forever()

# Function to handle WebSocket connection closure
def on_close(ws):
    global csv_writer, csv_file
    print("WebSocket connection closed")
    csv_writer = None
    if csv_file is not None:
        csv_file.close()

if __name__ == "__main__":
    symbol = 'BTCUSDT'
    # Create the initial CSV file
    csv_file_path = create_csv_file(symbol)

    # Start the WebSocket thread in a separate thread
    ws_thread = threading.Thread(target=websocket_thread)
    ws_thread.start()

    # Periodically save the CSV file every 10 seconds
    save_interval = 10
    save_csv_periodically(csv_file_path, symbol, save_interval)


