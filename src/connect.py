import json
import websockets
import gzip
import io
import asyncio

async def send_ping(websocket: websockets.ClientConnection):
    ping_message = {
        "method": "server.ping",
        "params": {},
        "id": 1
    }
    while True:
        try:
            await websocket.send(json.dumps(ping_message))
            await asyncio.sleep(30)  # Send ping every 30 seconds
        except Exception as e:
            print(f"Error sending ping: {e}")
            break


async def connect(exchange, callback, uri, filename, sub_message=None, is_coinex=False):
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                print(f"Connected to {exchange[0]}")
                if is_coinex:
                    ping_task = asyncio.create_task(send_ping(websocket))

                if sub_message is not None:
                    sub_msg = json.dumps(sub_message).encode('utf-8')  # Ensure the message is in bytes
                    await websocket.send(sub_msg)

                while True:
                    update = await websocket.recv()

                    # Check if the data is bytes and possibly compressed (CoinEx uses GZIP)
                    if isinstance(update, bytes):
                        try:
                            # CoinEx uses GZIP for data compression, check if it's compressed
                            if update[0:3] == b'\x1f\x8b\x08':  # GZIP header signature
                                with gzip.GzipFile(fileobj=io.BytesIO(update)) as f:
                                    update = f.read().decode('utf-8')  # Decompress and decode the data as UTF-8
                            else:
                                update = update.decode('utf-8')  # If it's not compressed, just decode it
                        except Exception as e:
                            print(f"Error during decompression or decoding: {e}")
                            continue  # Skip this message or handle differently

                    # Now process the data with json.loads
                    data = json.loads(update)
                    callback(data, filename)

        except Exception as e:
            print(f"Error in WebSocket connection: {exchange[0]} : {e}")
            # Optionally, add reconnect logic or delay before retrying
            await asyncio.sleep(5)  # Wait before reconnecting
