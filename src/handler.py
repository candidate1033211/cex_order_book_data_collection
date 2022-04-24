import json, datetime, requests
from errors import FlushingTimeoutError, ConnectionFailedError 

"""Functions to handle the data and write it to the database"""


def build_data_tuple(resp):
    """handles the raw data from CEX websocket and converts it into
     a string that will form a part of the SQL query
    """
    data = resp["data"]
    ID = data["id"]
    pair = data["pair"]
    time = data["time"] * 1000 # convert to nanosec
    if not data["asks"]: # only bids present
        side = 1
        size = data["bids"][0][-1]
        price = data["bids"][0][0]
    elif not data["bids"]: # only asks present
        side = -1
        size = data["asks"][0][-1]
        price = data["asks"][0][0]
    elif (not data["bids"]) and (not data["asks"]): # neither bids nor asks present
        side = 0
        size = 0
        price = 0
    else: # if both bids and asks are present
        side_bid = 1
        size_bid = data["bids"][0][-1]
        price_bid = data["bids"][0][0]
        side_ask = -1
        size_ask = data["asks"][0][-1]
        price_ask = data["asks"][0][0]
        tuple_bid = (ID, pair, side_bid, size_ask, price_ask, time)
        tuple_ask = (ID, pair, side_ask, size_ask, price_ask, time)
        return str(tuple_bid) + ", " + str(tuple_ask) # in this scenario we return
                                                      #  two queries essentially and func terminates
    
    data_tuple = (ID, pair, side, size, price, time)
    return str(data_tuple)

def flush(db_name, buffer, db_url="http://localhost:9000/"):
    """Takes a buffer of data points and writes it to the data base"""

    value_queue = [build_data_tuple(resp) for resp in buffer]
    query = "INSERT INTO " + db_name + " VALUES "
    query += ', '.join(value_queue)
    r = requests.get(db_url + "exec?query=" + query)


async def handle(db_name, ws, db_url = "http://localhost:9000/"):
    """Handles order book stream but bundeling data in a buffer for 10 seconds then flushing"""
    """Function raises exceptions if the stream is not present or the buffer has not been flushed for 40 seconds"""
    last_flushed = datetime.datetime.now().timestamp()
    buffer = []
    while True:
        resp = await ws.recv()
        resp = json.loads(resp)

        # if you made a connection but there's no stream (due to server faults)
        if (resp["e"] == "order-book-subscribe") and ('error' in resp["data"]):
            raise ConnectionFailedError("Try Reconnecting to the Server!")
        if resp["e"] == "ping":
            await ws.send(json.dumps({"e" : "pong"}))
            #print("Hello")
        if resp["e"] == "md_update":
            #build_data_tuple(resp)
            buffer.append(resp)
        if datetime.datetime.now().timestamp() > last_flushed + 10:
            flush(db_name, buffer, db_url)
            buffer = []
            last_flushed = datetime.datetime.now().timestamp()
        # if things are not being written raise error 
        if datetime.datetime.now().timestamp() - last_flushed > 40:
            raise FlushingTimeoutError("No data has been saved for the past 40 seconds!")
