#Source: https://medium.com/coinmonks/coinbase-pro-websockets-dea21da31d7b
#Modified to use Coinbase exchange instead of pro, and to write to Deephaven tables
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.time import to_datetime

from websocket import create_connection, WebSocketConnectionClosedException

import json, time
from threading import Thread

def coinbase_time_to_datetime(strn):
    return to_datetime(strn[0:-1] + " UTC")

dtw_column_converter = {
    'size': float,
    'price': float,
    'time': coinbase_time_to_datetime
}

dtw_columns = {
    'type': dht.string,
    'trade_id': dht.int_,
    'maker_order_id': dht.string,
    'taker_order_id': dht.string,
    'side': dht.string,
    'size': dht.float_,
    'price': dht.float_,
    'product_id': dht.string,
    'sequence': dht.int_,
    'time': dht.DateTime
}


def coinbase_websocket_connector(product_ids, channels, types):
    """
    Connects to the Coinbase websocket feed and writes the
    data to the Deephaven table

    Parameters:
        product_ids (list<str>): A list of products to track
        channels (list<str>): A list of channels to listen to
        types (list<str>): A list of types from the websocket connection to track
    Returns:
        None
    """
    ws = None
    thread = None
    thread_running = False
    thread_keepalive = None
    dtw = DynamicTableWriter(dtw_columns)

    def websocket_thread():
        global ws

        ws = create_connection("wss://ws-feed.exchange.coinbase.com")
        ws.send(
            json.dumps(
                {
                    "type": "subscribe",
                    "product_ids": product_ids,
                    "channels": channels,
                }
            )
        )

        thread_keepalive.start()
        while not thread_running:
            try:
                data = ws.recv()
                if data != "":
                    msg = json.loads(data)
                else:
                    msg = {}
            except ValueError as e:
                print(e)
                print("{} - data: {}".format(e, data))
            except Exception as e:
                print(e)
                print("{} - data: {}".format(e, data))
            else:
                if "type" in msg and msg["type"] in types:
                    row_to_write = []
                    for key in msg.keys():
                        value = None
                        if key in dtw_column_converter:
                            value = dtw_column_converter[key](msg[key])
                        else:
                            value = msg[key]
                        row_to_write.append(value)
                    dtw.write_row(*row_to_write)

        try:
            if ws:
                ws.close()
        except WebSocketConnectionClosedException:
            pass
        finally:
            thread_keepalive.join()

    def websocket_keepalive(interval=30):
        global ws
        while ws.connected:
            ws.ping("keepalive")
            time.sleep(interval)

    thread = Thread(target=websocket_thread)
    thread_keepalive = Thread(target=websocket_keepalive)
    thread.start()

    return dtw.table
