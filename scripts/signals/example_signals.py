import json
import random
import time
import uuid

import redis

from src.constants.signal import SignalStream, SignalType
from src.utils.logger import get_logger
from src.constants.config import RedisConfig

logger = get_logger('SignalsStream')

example_signal = {
    "id": str(uuid.uuid4()),
    "signalType": SignalType.add_liquidity,
    "chainId": "0x38",
    "transactionHash": "0x9be012327f22e5f46a17ed2e6953a9fbd98af7dfc8581afc03a792545eb67a0d",
    "blockNumber": 47850066,
    "timestamp": int(time.time()) - random.randint(1, 1000),
    "explorerUrl": "https://bscscan.com/tx/0x9be012327f22e5f46a17ed2e6953a9fbd98af7dfc8581afc03a792545eb67a0d",
    "description": "",
    "projectId": "pancakeswap-amm",
    "projectName": "PancakeSwap",
    "projectImgUrl": "https://s2.coinmarketcap.com/static/img/exchanges/64x64/1165.png",
    "projectType": "Dex",
    "assets": [
        {
            "name": "USDC",
            "symbol": "USDC",
            "imgUrl": "https://coin-images.coingecko.com/coins/images/6319/large/usdc.png"
        },
        {
            "name": "Tether",
            "symbol": "USDT",
            "imgUrl": "https://coin-images.coingecko.com/coins/images/35021/large/USDT.png"
        }
    ],
    "value": 414.16873
}


def example_push():
    _r = redis.from_url(RedisConfig.CONNECTION_URL, decode_responses=True)
    print('Example Signal', example_signal)

    # example_signal['assets'] = json.dumps(example_signal['assets'])
    message = {'msg': json.dumps(example_signal)}

    # reformated_signal = {k: v if v is not None else "null" for k, v in example_signal.items()}
    _r.xadd(SignalStream.stream_name, message, maxlen=SignalStream.max_len)


def main():
    while True:
        example_push()
        time.sleep(10)


if __name__ == '__main__':
    main()
