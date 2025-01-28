import json
from bybit.callback import callback
from connect import connect
import asyncio

async def bybit_connect(exchange):
    try:
        symbols = exchange[1]['cryptos']
        base_uri = exchange[1]['uri']
        streams = exchange[1]['streams']
        bybit_listners = []

        for stream in streams:
            args = []
            for symbol in symbols:
                stream_code = f'{stream}.{symbol}'
                args.append(stream_code)

            submsg = {
                    "op": "subscribe",
                    "args": args
                }

        listener = asyncio.create_task(connect(exchange, callback, base_uri, "filename", submsg))
        bybit_listners.append(listener)

        return bybit_listners
    except Exception as e:
        print(f"Error in binance_connect: {e}")
        return []
