import asyncio
import time
from binance.binance import binance_connect
from bybit.bybit import bybit_connect
from coinex.coinex import coinex_connect

async def run_listeners(exchanges: dict):
    listeners = []

    # Iterate over each exchange
    for exchange in exchanges.items():
        listener = []  # Default empty listener list
        # Check which exchange and call the corresponding connect function
        if exchange[0] == "binance":
            listener = await binance_connect(exchange)
        elif exchange[0] == "bybit_spot":
            listener = await bybit_connect(exchange)
        elif exchange[0] == "bybit_futures":
            listener = await bybit_connect(exchange)
        elif exchange[0] == "coinex_spot":
            listener = await coinex_connect(exchange)
        elif exchange[0] == "coinex_futures":
            listener = await coinex_connect(exchange)

        # Only extend listeners if listener is valid (i.e., not None)
        if listener:
            listeners.extend(listener)
        else:
            print(f"Warning: No listeners returned for {exchange[0]}")

    # If listeners is not empty, gather them
    if listeners:
        await asyncio.gather(*listeners)
    else:
        print("No listeners to run.")

async def main(scrape_data):
    try:
        await run_listeners(scrape_data)
    except KeyboardInterrupt:
        print("KeyboardInterrupt caught, stopping listeners...")
        # Here, you can implement the cancellation of tasks if needed
        # If the listeners are long-running tasks, you can cancel them.
        for task in asyncio.all_tasks():
            task.cancel()

if __name__ == "__main__":
    scrape_data = {
        "binance": {
            "uri": "wss://data-stream.binance.vision/stream",
            "cryptos": ["BTCUSDT", "ETHUSDT"],
            "streams": ["ticker", "trade"]
        },
        "bybit_spot": {
            "uri": "wss://stream.bybit.com/v5/public/spot",
            "cryptos": ["BTCUSDT", "ETHUSDT"],
            "streams": ["tickers", "publicTrade"]
        },
        "bybit_futures": {
            "uri": "wss://stream.bybit.com/v5/public/linear",
            "cryptos": ["BTCUSDT", "ETHUSDT"],
            "streams": ["tickers", "publicTrade"]
        },
        "coinex_spot": {
            "uri": "wss://socket.coinex.com/v2/spot",
            "cryptos": ["BTCUSDT", "ETHUSDT"],
            "streams": ["bbo", "trade"]
        },
        "coinex_futures": {
            "uri": "wss://socket.coinex.com/v2/futures",
            "cryptos": ["BTCUSDT", "ETHUSDT"],
            "streams": ["bbo", "trade"]
        }
    }

    # Start the listeners with graceful shutdown on KeyboardInterrupt
    asyncio.run(main(scrape_data))
