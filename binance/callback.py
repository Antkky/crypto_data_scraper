import os

def callback(data, filename):
    rowdata = data["data"]
    row = f'{rowdata["E"]},{rowdata["o"]},{rowdata["h"]},{rowdata["l"]},{rowdata["c"]},{rowdata["v"]},{rowdata["n"]},'
    symbol_path = os.path.join('./data/', str(data["data"]["s"]))
    file_path = os.path.join(symbol_path, filename)
    with open(file_path, 'a') as f:
        f.write(f"{row}\n")
