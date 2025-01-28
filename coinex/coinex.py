import asyncio
import json
from coinex.callback import callback
from connect import connect

async def send_ping(websocket):
    ping_message = {
        "method": "server.ping",
        "params": {},
        "id": 1
    }

    while True:
        try:
            # Send the ping message as a JSON string
            await websocket.send(json.dumps(ping_message))
            await asyncio.sleep(30)  # Send ping every 30 seconds (adjust as needed)
        except Exception as e:
            print(f"Error sending ping: {e}")
            break

async def coinex_connect(exchange):
    try:
        symbols = exchange[1]['cryptos']
        base_uri = exchange[1]['uri']
        streams = exchange[1]['streams']

        if not callable(callback):
            print("Invalid callback function.")
            return []

        coinex_listeners = []

        print(f"Connected to {exchange[0]}")

        for stream in streams:
            submsg = {
                "method": f"{stream}.subscribe",
                    "params": {"market_list": symbols},
                    "id": 1
                }
            filename = f'idk_{exchange[0]}_{stream}.csv'
            listener = await connect(exchange, callback, base_uri, filename, submsg, is_coinex=True)
            coinex_listeners.append(listener)
            # Keep the main connection alive while processing data
            await asyncio.gather(*coinex_listeners)

    except Exception as e:
        print(f"Error in coinex_connect: {e}")
        return []
