import json
from authentication import auth_request


async def produce(key, secret, ws):
    # subscribes to the order book 
    auth = auth_request(key, secret)
    order_book_sub = json.dumps({"e": "order-book-subscribe",
                                 "data": {"pair": ["BTC","USD"],
                                          "subscribe": "true",
                                          "depth": -1},
                                 "oid": "1435927928274_3_order-book-subscribe"})
    await ws.send(auth)
    await ws.send(order_book_sub)

