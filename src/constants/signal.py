DEX_LENDING_EVENTS_THRESHOLD = {
    "add_liquidity": 1000000,
    "remove_liquidity": 1000000,
    "deposit": 1000000,
    "withdraw": 1000000,
    "borrow": 1000000,
    "repay": 1000000,
    "liquidate": 20000,
    "new_listing": 20000,
    "swap": 1000000,
}

JUP_PERPS_THRESHOLD = {
    "open_position": 1000000,
    "close_position": 1000000,
    "liquidate": 20000,
}

AUTO_INVEST_VALUE = 1  # $1
AUTO_INVEST_INPUT_TOKEN_ADDRESS = '0x6969696969696969696969696969696969696969'  # WSOL
AUTO_INVEST_INPUT_TOKEN_DECIMALS = 18

IGNORE_SWAP_TOKENS = {
    "0x138de": [
        '0x6969696969696969696969696969696969696969',  # Wrapped Bera
        '0xff0a636dfc44bb0129b631cdd38d21b613290c98',  # Hold Station,
        '0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34',  # USDe
        '0xfcbd14dc51f0a4d49d5e53c2e0950e0bc26d0dce',  # HONEY
        '0xff12470a969dd362eb6595ffb44c82c959fe9acc',  # USDa
        '0x779ded0c9e1022225f8e0630b35a9b54be713736',  # USDT0
        '0x5b82028cfc477c4e7dda7ff33d59a23fa7be002a',  # MIM
        '0x09d4214c03d01f49544c0448dbe3a27f768f2b34',  # rUSD
        '0x2840f9d9f96321435ab0f977e7fdbf32ea8b304f',  # sUSDa
        '0x1ce0a25d13ce4d52071ae7e02cf1f6606f4c79d3',  # NECT
        '0x211cc4dd073734da055fbf44a2b4667d5e5fe5d2',  # sUSDe
        '0x549943e04f40284185054145c6e4e9568c1d3241',  # USDC.e,
        '0x2f6f07cdcf3588944bf4c42ac74ff24bf56e7590',  # WETH
        '0x0555e30da8f98308edb960aa94c0db47230d2b9c',  # WBTC
    ],
    "0x38": [
        '0xe9e7cea3dedca5984780bafc599bd69add087d56',  # BUSD (Binance USD)
        '0x55d398326f99059ff775485246999027b3197955',  # USDT (Tether on BSC)
        '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d',  # USDC (USD Coin on BSC)
        '0x2170ed0880ac9a755fd29b2688956bd959f933f8',  # WETH (Binance-Peg WETH)
        '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c',  # BTCB (Binance Bitcoin)
        '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c',  # WBNB (Wrapped BNB)
        '0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd',  # LINK (Chainlink on BSC)
        '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3',  # DAI (Binance-Peg DAI)
        '0x14016e85a25aeb13065688cafb43044c2ef86784',  # TUSD (Bridged TrueUSD)
    ]
}

class SignalType:
    add_liquidity = 'add_liquidity'
    auto_invest = 'auto_invest'


class SignalStream:
    stream_name = 'signal_stream'

    max_len = 100

