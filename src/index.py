
import asyncio
import websockets
import json
import os


# Time, ID, Price, Quantity, Passive Bid
def trade_callback(data, filename):
    rowdata = data["data"]
    row = f'{rowdata["T"]},{rowdata["t"]},{rowdata["p"]},{rowdata["q"]},{rowdata["m"]}'
    symbol_path = os.path.join('./data/', str(data["data"]["s"]))
    file_path = os.path.join(symbol_path, filename)
    with open(file_path, 'a') as f:
        f.write(f"{row}\n")

# Time, Open, High, Low, Close, Volume, Trade Count
def ticker_callback(data, filename):
    rowdata = data["data"]
    row = f'{rowdata["E"]},{rowdata["o"]},{rowdata["h"]},{rowdata["l"]},{rowdata["c"]},{rowdata["v"]},{rowdata["n"]},'
    symbol_path = os.path.join('./data/', str(data["data"]["s"]))
    file_path = os.path.join(symbol_path, filename)
    with open(file_path, 'a') as f:
        f.write(f"{row}\n")

async def connect_stream(uri, callback, filename):
    async with websockets.connect(uri) as websocket:
        print(f"Connected to {uri}")
        while True:
            message = await websocket.recv()
            data = json.loads(message)

            callback(data, filename)

async def start_listeners(cryptos: list, streams: list):
    buri = 'wss://data-stream.binance.vision/stream'
    
    listeners = []

    # loop through and create listeners for each stream
    for symbol in cryptos:
        for stream in streams:
            uri = f"{buri}?streams={stream_code}"
            callback = None
            filename = f'{symbol}_{stream}.csv'
            stream_code = f"{symbol.lower()}@{stream}"

            if stream == "trade":
                callback = trade_callback
                
            elif stream == "ticker":
                callback = ticker_callback

            if callback is not None:
                listener = asyncio.create_task(connect_stream(uri, callback, filename))
                listeners.append(listener)


    await asyncio.gather(*listeners)

def prepare_files(cryptos:list, streams:list):
    data_path = './data'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    for crypto in cryptos:
        crypto_path = os.path.join(data_path, crypto)
        if not os.path.exists(crypto_path):
            os.makedirs(crypto_path)
        
        for stream in streams:
            filename = f'{crypto}_{stream}.csv'
            file_path = os.path.join(crypto_path, filename)
            if not os.path.exists(file_path):
                with open(file_path, "w") as f:
                    f.write("Time,ID,Price,Quantity,Passive Bid\n")
            

if __name__ == "__main__":
    cryptos_to_scrape = ["BTCUSDT", "ETHUSDT"]
    streams_to_scrape = ["trade",'tickers']
    
    prepare_files(cryptos_to_scrape, streams_to_scrape)
    asyncio.run(start_listeners(cryptos_to_scrape, streams_to_scrape))