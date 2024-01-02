# Binance WebSocket

This repository provides a Python implementation to stream data from Binance using WebSocket.

## Documentation

For detailed documentation regarding the Binance WebSocket API, please refer to the [official Binance API documentation](https://binance-docs.github.io/apidocs/spot/en/#introduction).

## Files Included

This repository includes the following files:

- `bookticker-async-single.py`: Streams real-time 'bookTicker' data asynchronously.
- `bookticker-single.py`: Streams real-time 'bookTicker' data using threading.
- `orderbook-depth-single.py`: Streams top-level 'depth' data using threading.

## Data Storage

The retrieved information is stored in CSV files, capturing data every 10 minutes. This allows capturing the entire day in multiple 10-minute snapshots for better analysis and retrieval.

Feel free to explore the code and adapt it to your specific requirements.

If you have any questions or need further assistance, please don't hesitate to reach out.

Happy coding!
