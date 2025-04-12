CURVE_POOL_EVENTS_ABI = [
    {
        "name": "TokenExchange",
        "inputs": [
            {
                "name": "buyer",
                "type": "address",
                "indexed": True
            },
            {
                "name": "sold_id",
                "type": "int128",
                "indexed": False
            },
            {
                "name": "tokens_sold",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "bought_id",
                "type": "int128",
                "indexed": False
            },
            {
                "name": "tokens_bought",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "TokenExchangeUnderlying",
        "inputs": [
            {
                "name": "buyer",
                "type": "address",
                "indexed": True
            },
            {
                "name": "sold_id",
                "type": "int128",
                "indexed": False
            },
            {
                "name": "tokens_sold",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "bought_id",
                "type": "int128",
                "indexed": False
            },
            {
                "name": "tokens_bought",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "fees",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "invariant",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "fees",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityOne",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_id",
                "type": "int128",
                "indexed": False
            },
            {
                "name": "token_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "coin_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityImbalance",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "fees",
                "type": "uint256[]",
                "indexed": False
            },
            {
                "name": "invariant",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[2]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[2]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[2]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[2]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityOne",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256",
                "name": "token_amount",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "coin_amount",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityImbalance",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[2]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[2]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[3]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[3]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[3]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[3]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityImbalance",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[3]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[3]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityOne",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "coin_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "TokenExchange",
        "inputs": [
            {
                "name": "buyer",
                "type": "address",
                "indexed": True
            },
            {
                "name": "sold_id",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "tokens_sold",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "bought_id",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "tokens_bought",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_price_scale",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[3]",
                "indexed": False
            },
            {
                "name": "fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_price_scale",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[3]",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityOne",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "coin_index",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "coin_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "approx_fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_price_scale",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "TokenExchange",
        "inputs": [
            {
                "name": "buyer",
                "type": "address",
                "indexed": True
            },
            {
                "name": "sold_id",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "tokens_sold",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "bought_id",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "tokens_bought",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[3]",
                "indexed": False
            },
            {
                "name": "fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[4]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[4]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[4]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[4]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidityImbalance",
        "inputs": [
            {
                "type": "address",
                "name": "provider",
                "indexed": True
            },
            {
                "type": "uint256[4]",
                "name": "token_amounts",
                "indexed": False
            },
            {
                "type": "uint256[4]",
                "name": "fees",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "invariant",
                "indexed": False
            },
            {
                "type": "uint256",
                "name": "token_supply",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[2]",
                "indexed": False
            },
            {
                "name": "fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_price_scale",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "RemoveLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[2]",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "AddLiquidity",
        "inputs": [
            {
                "name": "provider",
                "type": "address",
                "indexed": True
            },
            {
                "name": "token_amounts",
                "type": "uint256[2]",
                "indexed": False
            },
            {
                "name": "fee",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "token_supply",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    }
]
