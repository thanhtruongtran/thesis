CURVE_ROUTER_ABI = [
    {
        "name": "Exchange",
        "inputs": [
            {
                "name": "sender",
                "type": "address",
                "indexed": True
            },
            {
                "name": "receiver",
                "type": "address",
                "indexed": True
            },
            {
                "name": "route",
                "type": "address[11]",
                "indexed": False
            },
            {
                "name": "swap_params",
                "type": "uint256[5][5]",
                "indexed": False
            },
            {
                "name": "pools",
                "type": "address[5]",
                "indexed": False
            },
            {
                "name": "in_amount",
                "type": "uint256",
                "indexed": False
            },
            {
                "name": "out_amount",
                "type": "uint256",
                "indexed": False
            }
        ],
        "anonymous": False,
        "type": "event"
    },
    {
        "stateMutability": "payable",
        "type": "fallback"
    },
    {
        "stateMutability": "nonpayable",
        "type": "constructor",
        "inputs": [
            {
                "name": "_weth",
                "type": "address"
            },
            {
                "name": "_stable_calc",
                "type": "address"
            },
            {
                "name": "_crypto_calc",
                "type": "address"
            }
        ],
        "outputs": []
    },
    {
        "stateMutability": "payable",
        "type": "function",
        "name": "exchange",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_amount",
                "type": "uint256"
            },
            {
                "name": "_min_dy",
                "type": "uint256"
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
        "stateMutability": "payable",
        "type": "function",
        "name": "exchange",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_amount",
                "type": "uint256"
            },
            {
                "name": "_min_dy",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
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
        "stateMutability": "payable",
        "type": "function",
        "name": "exchange",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_amount",
                "type": "uint256"
            },
            {
                "name": "_min_dy",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
            },
            {
                "name": "_receiver",
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
        "name": "get_dy",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_amount",
                "type": "uint256"
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
        "name": "get_dy",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_amount",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
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
        "name": "get_dx",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_out_amount",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
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
        "name": "get_dx",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_out_amount",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
            },
            {
                "name": "_base_pools",
                "type": "address[5]"
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
        "name": "get_dx",
        "inputs": [
            {
                "name": "_route",
                "type": "address[11]"
            },
            {
                "name": "_swap_params",
                "type": "uint256[5][5]"
            },
            {
                "name": "_out_amount",
                "type": "uint256"
            },
            {
                "name": "_pools",
                "type": "address[5]"
            },
            {
                "name": "_base_pools",
                "type": "address[5]"
            },
            {
                "name": "_base_tokens",
                "type": "address[5]"
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
        "name": "version",
        "inputs": [],
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ]
    }
]
