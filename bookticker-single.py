"""
Binance Book Ticker Data Collector
- Continuously captures BTCUSDT book ticker data using the official Binance API
- Saves data to a simple append-only text file
- Designed for long-term data collection (many weeks)

Installation:
    pip install python-binance
"""

import os
import time
import signal
import threading
from datetime import datetime, timezone
from binance import ThreadedWebsocketManager

SYMBOL = 'BTCUSDT'


class BookTickerCollector:
    def __init__(self, symbol):
        self.symbol = symbol
        self.twm = None
        self.data_file = None
        self.stream_id = None

        # Create data directory structure
        self.base_dir = f"data/{self.symbol}"
        os.makedirs(self.base_dir, exist_ok=True)

        # Initialize the file
        self._setup_file()

    def _setup_file(self):
        """Setup the append-only data file"""
        folder_path = f"{self.base_dir}/"
        os.makedirs(folder_path, exist_ok=True)

        filename = f'{folder_path}/book_ticker_{self.symbol}.txt'

        # Open file in append mode
        self.data_file = open(filename, mode='a', encoding='utf-8')

        # Write header if file is new (empty)
        if os.path.getsize(filename) == 0:
            header = "timestamp|bid_price|bid_qty|ask_price|ask_qty\n"
            self.data_file.write(header)

        print(f"Appending data to: {filename}")

        return filename

    def process_message(self, msg):
        """Process incoming book ticker messages"""

        try:
            # Extract the relevant data - minimal processing
            timestamp = datetime.now(timezone.utc).isoformat()

            # Write directly without converting to float to save processing time
            # Format: timestamp|bid_price|bid_qty|ask_price|ask_qty
            self.data_file.write(f"{timestamp}|{msg['b']}|{msg['B']}|{msg['a']}|{msg['A']}\n")

        except Exception as e:
            print(f"Error processing message: {e}")

    def start(self):
        """Start the book ticker data collection"""
        try:
            # Initialize the WebSocket manager with a larger queue size
            self.twm = ThreadedWebsocketManager(api_key=None, api_secret=None, tld='com', max_queue_size=100000)
            self.twm.start()

            # Start the book ticker socket
            self.stream_id = self.twm.start_symbol_book_ticker_socket(
                symbol=self.symbol,
                callback=self.process_message
            )

            print(f"Started book ticker collection for {self.symbol}")

        except Exception as e:
            print(f"Error starting collector: {e}")
            raise


def signal_handler(sig, frame):
    collector.data_file.flush()
    collector.twm.stop()
    collector.data_file.close()
    print('You pressed Ctrl+C!')
    exit(0)


if __name__ == "__main__":

    try:
        print(f"Starting book ticker data collection for {SYMBOL}")
        # Create and start the collector
        collector = BookTickerCollector(SYMBOL)
        collector.start()

        signal.signal(signal.SIGINT, signal_handler)

        # Keep the main thread running
        while True:
            time.sleep(1)

    except Exception as e:
        print(f"Fatal error: {e}")
        running = False
