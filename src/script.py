import websockets, asyncio
from producer import produce
from handler import handle 

"""connecting and producing a stream of data"""


key = "21gxBCYiaAitBaG4PjJtbWcMXJ0"
secret = "v4QX2EkVVj7OjTMlvilYJ6l39TA"


async def main():
    while True: # we do that to ensure the constant connection
        try:
            async with websockets.connect("wss://ws.cex.io/ws/") as ws:
                await produce(key, secret, ws)
                await handle("cex_order_book", ws, db_url="http://167.71.130.224:9000/")
        except:
            print("Connection Lost. Reconnecting....")

asyncio.run(main())

