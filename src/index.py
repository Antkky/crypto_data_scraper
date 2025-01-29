import asyncio
import time
import json
import websockets
import gzip
import os
import csv
import io
from collections import deque

BUFFER_SIZE = 500
FLUSH_INTERVAL = 5

buffers = {}

TRADE_SCHEMA = ["timestamp", "exchange", "symbol", "price", "amount", "side"]
TICK_SCHEMA = ["timestamp", "exchange", "symbol", "bid price", "ask price", "bid volume", "ask volume"]

async def flush_buffer(exchange_name, symbol):
    """Flush buffer to CSV periodically."""
    key = (exchange_name, symbol)
    if key in buffers and buffers[key]:
        datapath = './data/'
        os.makedirs(datapath, exist_ok=True)  # Ensure directory exists
        filename = f"{exchange_name}_{symbol}_trades.csv"
        path = os.path.join(datapath, filename)

        data_list = buffers[key]
        if not data_list:
            return

        try:
            with open(path, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=TRADE_SCHEMA)
                if file.tell() == 0:
                    writer.writeheader()  # Write header if new file
                writer.writerows(data_list)  # Write all buffered data
            buffers[key] = []  # Clear buffer
        except Exception as e:
            print(f"Error saving to CSV for {exchange_name}: {e}")

async def buffer_data(exchange_name: str, symbol: str, data: dict):
    """Buffer incoming data before writing to CSV."""
    key = (exchange_name, symbol)
    if key not in buffers:
        buffers[key] = []

    # Standardized trade format
    trade_entry = {
        "timestamp": data.get("timestamp", time.time()),  # Use provided timestamp or fallback
        "exchange": exchange_name,
        "symbol": symbol,
        "price": data.get("price"),
        "amount": data.get("amount"),
        "side": data.get("side")  # Buy/Sell direction
    }

    buffers[key].append(trade_entry)

    # Flush if buffer reaches threshold
    if len(buffers[key]) >= BUFFER_SIZE:
        await flush_buffer(exchange_name, symbol)

async def periodic_flush():
    """Flush all buffers every X seconds to avoid data loss."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL)
        for (exchange_name, symbol) in list(buffers.keys()):
            await flush_buffer(exchange_name, symbol)

async def handle_binance(message):
    """Extract trade data from Binance messages."""
    if "data" in message and "p" in message["data"]:
        trade_data = {
            "timestamp": message["data"]["T"] / 1000,  # Convert ms to seconds
            "price": float(message["data"]["p"]),
            "amount": float(message["data"]["q"]),
            "side": "buy" if message["data"]["m"] is False else "sell",
        }
        await buffer_data("binance", "btcusdt", trade_data)

async def handle_bitfinex(message):
    """Extract trade data from Bitfinex messages."""
    print("Bitfinex Message:", message)  # Debugging output
    if isinstance(message, list) and len(message) > 2 and isinstance(message[1], list):
        trade_data = {
            "timestamp": message[1][1] / 1000,  # Convert ms to seconds
            "price": float(message[1][3]),
            "amount": float(abs(message[1][2])),  # Positive for buy, negative for sell
            "side": "buy" if message[1][2] > 0 else "sell",
        }
        await buffer_data("bitfinex", "BTCUSD", trade_data)

async def handle_bybit(message):
    """Extract trade data from Bybit messages."""
    if "data" in message and isinstance(message["data"], list):
        for trade in message["data"]:
            trade_data = {
                "timestamp": trade["T"] / 1000,  # Convert ms to seconds
                "price": float(trade["p"]),
                "amount": float(trade["v"]),
                "side": "buy" if trade["S"] == "Buy" else "sell",
            }
            await buffer_data("bybit", "BTCUSDT", trade_data)

async def handle_coinex(message):
    """Extract trade data from CoinEx messages."""
    if "params" in message and "data" in message["params"]:
        for trade in message["params"]["data"]:
            trade_data = {
                "timestamp": trade["date_ms"] / 1000,  # Convert ms to seconds
                "price": float(trade["price"]),
                "amount": float(trade["amount"]),
                "side": "buy" if trade["type"] == "buy" else "sell",
            }
            await buffer_data("coinex", "BTCUSDT", trade_data)

EXCHANGES = {
    "wss://data-stream.binance.vision/stream": {
        "subscribe": [{"method": "SUBSCRIBE", "params": ["btcusdt@trade"], "id": 1}],
        "exchange_name": "binance",
        "callback": handle_binance
    },
    "wss://api-pub.bitfinex.com/ws/2": {
        "subscribe": [{"event": "subscribe", "channel": "trades", "symbol": "tBTCUSD"}],
        "exchange_name": "bitfinex",
        "callback": handle_bitfinex
    },
    "wss://stream.bybit.com/v5/public/spot": {
        "subscribe": [{"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}],
        "exchange_name": "bybit_spot",
        "callback": handle_bybit
    },
    "wss://socket.coinex.com/v2/spot": {
        "subscribe": [{"method": "deals.subscribe", "params": {"market_list": ["BTCUSDT"]}, "id": 1}],
        "exchange_name": "coinex_spot",
        "callback": handle_coinex
    }
}

async def handle_websocket(exchange_url, config):
    """Handle WebSocket connection with buffering and keepalive ping/pong."""
    exchange_name = config.get("exchange_name", "unknown")
    callback = config.get("callback", None)

    while True:
        try:
            async with websockets.connect(exchange_url) as ws:
                # Send ping to keep connection alive
                async def send_ping(config):
                    while True:
                        await asyncio.sleep(30)  # Send ping every 30 seconds
                        await ws.ping()
                        msg = config.get("ping", [])
                        if msg:
                            await ws.send(json.dumps(msg))

                # Start keepalive ping thread
                asyncio.create_task(send_ping(config))

                for msg in config.get("subscribe", []):
                    await ws.send(json.dumps(msg))

                async for message in ws:
                    if isinstance(message, bytes):
                        try:
                            if message[:3] == b'\x1f\x8b\x08':  # GZIP header check
                                with gzip.GzipFile(fileobj=io.BytesIO(message)) as f:
                                    message = f.read().decode('utf-8')
                            else:
                                message = message.decode('utf-8')
                        except Exception as e:
                            print(f"Decompression error: {e}")
                            continue

                    try:
                        data = json.loads(message)
                    except json.JSONDecodeError as e:
                        print(f"JSON Decode error from {exchange_name}: {e}")
                        continue

                    if callback:
                        await callback(data)

        except Exception as e:
            print(f"Error with {exchange_name}: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

async def main():
    tasks = [handle_websocket(url, config) for url, config in EXCHANGES.items()]
    tasks.append(periodic_flush())
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
