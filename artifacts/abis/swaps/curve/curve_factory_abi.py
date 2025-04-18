CURVE_FACTORY_ABI = [
    {
        "name": "TricryptoPoolDeployed",
        "inputs": [
            {
                "name": "pool",
                "type": "address",
                "indexed": False
            },
            {
                "name": "name",
                "type": "string",
                "indexed": False
            },
            {
                "name": "symbol",
                "type": "string",
                "indexed": False
            },
            {
                "name": "weth",
                "type": "address",
                "indexed": False
            },
            {
                "name": "coins",
                "type": "address[3]",
                "indexed": False
            },
            {
                "name": "math",
                "type": "address",
                "indexed": False
            },
            {
                "name": "salt",
                "type": "bytes32",
                "indexed": False
            },
            {
                "name": "packed_precisions",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_A_gamma",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_fee_params",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_rebalancing_params",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "packed_prices",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "deployer",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "LiquidityGaugeDeployed",
        "inputs": [
            {
                "name": "pool",
                "type": "address",
                "indexed": False
            },
            {
                "name": "gauge",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "UpdateFeeReceiver",
        "inputs": [
            {
                "name": "_old_fee_receiver",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_fee_receiver",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "UpdatePoolImplementation",
        "inputs": [
            {
                "name": "_implemention_id",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "_old_pool_implementation",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_pool_implementation",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "UpdateGaugeImplementation",
        "inputs": [
            {
                "name": "_old_gauge_implementation",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_gauge_implementation",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "UpdateMathImplementation",
        "inputs": [
            {
                "name": "_old_math_implementation",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_math_implementation",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "UpdateViewsImplementation",
        "inputs": [
            {
                "name": "_old_views_implementation",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_views_implementation",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "name": "TransferOwnership",
        "inputs": [
            {
                "name": "_old_owner",
                "type": "address",
                "indexed": False
            },
            {
                "name": "_new_owner",
                "type": "address",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "stateMutability": "nonpayable",
        "type": "constructor",
        "inputs": [
            {
                "name": "_fee_receiver",
                "type": "address"
            },
            {
                "name": "_admin",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "deploy_pool",
        "inputs": [
            {
                "name": "_name",
                "type": "string"
            },
            {
                "name": "_symbol",
                "type": "string"
            },
            {
                "name": "_coins",
                "type": "address[3]"
            },
            {
                "name": "_weth",
                "type": "address"
            },
            {
                "name": "implementation_id",
                "type": "uint256"
            },
            {
                "name": "A",
                "type": "uint256"
            },
            {
                "name": "gamma",
                "type": "uint256"
            },
            {
                "name": "mid_fee",
                "type": "uint256"
            },
            {
                "name": "out_fee",
                "type": "uint256"
            },
            {
                "name": "fee_gamma",
                "type": "uint256"
            },
            {
                "name": "allowed_extra_profit",
                "type": "uint256"
            },
            {
                "name": "adjustment_step",
                "type": "uint256"
            },
            {
                "name": "ma_exp_time",
                "type": "uint256"
            },
            {
                "name": "initial_prices",
                "type": "uint256[2]"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "deploy_gauge",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "set_fee_receiver",
        "inputs": [
            {
                "name": "_fee_receiver",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "set_pool_implementation",
        "inputs": [
            {
                "name": "_pool_implementation",
                "type": "address"
            },
            {
                "name": "_implementation_index",
                "type": "uint256"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "set_gauge_implementation",
        "inputs": [
            {
                "name": "_gauge_implementation",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "set_views_implementation",
        "inputs": [
            {
                "name": "_views_implementation",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "set_math_implementation",
        "inputs": [
            {
                "name": "_math_implementation",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "commit_transfer_ownership",
        "inputs": [
            {
                "name": "_addr",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "nonpayable",
        "type": "function",
        "name": "accept_transfer_ownership",
        "inputs": [],
        "outputs": []
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "find_pool_for_coins",
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "find_pool_for_coins",
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "i",
                "type": "uint256"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_coins",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address[3]"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_decimals",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "uint256[3]"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_balances",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "uint256[3]"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_coin_indices",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            },
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            },
            {
                "name": "",
                "type": "uint256"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_gauge",
        "inputs": [
            {
                "name": "_pool",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "get_market_counts",
        "inputs": [
            {
                "name": "coin_a",
                "type": "address"
            },
            {
                "name": "coin_b",
                "type": "address"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "admin",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "future_admin",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "fee_receiver",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "pool_implementations",
        "inputs": [
            {
                "name": "arg0",
                "type": "uint256"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "gauge_implementation",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "views_implementation",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "math_implementation",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "pool_count",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ]
    },
    {
        "stateMutability": "view",
        "type": "function",
        "name": "pool_list",
        "inputs": [
            {
                "name": "arg0",
                "type": "uint256"
            }
        ],
        "outputs": [
            {
                "name": "",
                "type": "address"
            }
        ]
    }
]
