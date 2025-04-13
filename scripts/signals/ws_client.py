import asyncio
import json

import websockets


async def listen_websocket():
    # For another test
    # signal_filter = "signal['value'] > 400 and 'USDC' in [a['symbol'] for a in signal['assets']]"
    #
    # import base58
    # encoded_filter = base58.b58encode(signal_filter.encode()).decode()
    #
    # uri = f"ws://0.0.0.0:8096/v3/defai/signals/ws?type=add_liquidity&filter={encoded_filter}"

    uri = "wss://develop.centic.io/stag/v3/defai/signals/ws"

    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            if message in ['ping', 'pong']:
                print(f'Ping: {message}')
                continue

            signal = json.loads(message)
            print(f"Received from server: {signal}")


async def main():
    clients = [listen_websocket(), listen_websocket()]
    await asyncio.gather(*clients)


if __name__ == "__main__":
    asyncio.run(listen_websocket())
