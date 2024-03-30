# Binance WebSocket

This repository provides a Python implementation for streaming Spot data from Binance using WebSocket. For other financial derivatives, please pay attention to the different API endpoints.

## Documentation

For detailed documentation regarding the Binance WebSocket API, please refer to the [official Binance API documentation](https://binance-docs.github.io/apidocs/spot/en/#introduction).

## Files Included

This repository includes the following files:

- `bookticker-async-single.py`: Streams real-time 'bookTicker' data asynchronously.
- `bookticker-single.py`: Streams real-time 'bookTicker' data using threading.
- `orderbook-depth-single.py`: Streams top-level 'depth' data using threading.
- `local-orderbook-data-parquet-single.py`: Saves depth updates asynchronously, and orderbook snapshots for construction of local orderbook as Parquet files.
- `local-orderbook-data-txt-single.py`: Saves depth updates asynchronously, and orderbook snapshots for construction of local orderbook as text files.

## Data Storage

The files (`bookticker-async-single.py`, `bookticker-single.py`, `orderbook-depth-single.py`) store retrieved information in CSV files, capturing data in 10-minute intervals. This allows capturing the entire day in multiple 10-minute intervals for better analysis and retrieval.

The `local-orderbook-data-parquet-single.py` saves Parquet files for depth updates and snapshots separately at approximately 10-minute intervals.

The `local-orderbook-data-txt-single.py` saves text files for depth updates and snapshots separately in one file per day.

##


Feel free to explore the code and adapt it to your specific requirements.

If you have any questions or need further assistance, please don't hesitate to reach out.

Happy coding!
