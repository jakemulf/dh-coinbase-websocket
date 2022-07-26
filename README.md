# dh-coinbase-websocket

This project shows how to connect to a websocket and write the websocket's data to Deephaven

## Launch

Simply run

```
docker-compose up
```

to launch the app

## Deephaven IDE

In the Deephaven IDE (http://localhost:10000), you can call `coinbase_websocket_connector()` to create a table containing the websocket data. Here's a few examples

```
btc_feed = coinbase_websocket_connector(['BTC-USD'], ["matches"], ["last_match", "match"])
eth_feed = coinbase_websocket_connector(['ETH-USD'], ["matches"], ["last_match", "match"])
btc_eth_feed = coinbase_websocket_connector(['ETH-USD', 'BTC-USD'], ["matches"], ["last_match", "match"])
```
