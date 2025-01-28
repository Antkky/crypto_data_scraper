from binance.callback import callback
from connect import connect
import asyncio

async def binance_connect(exchange):
    try:
        symbols = exchange[1]['cryptos']
        base_uri = exchange[1]['uri']
        streams = exchange[1]['streams']
        binance_listeners = []

        for symbol in symbols:
            for stream in streams:
                stream_code = f"{symbol.lower()}@{stream}"
                uri = f'{base_uri}?streams={stream_code}'

                filename = f'{symbol}_{stream}.csv'
                listener = asyncio.create_task(connect(exchange, callback, uri, filename))
                binance_listeners.append(listener)

        return binance_listeners
    except Exception as e:
        print(f"Error in binance_connect: {e}")
        return []
