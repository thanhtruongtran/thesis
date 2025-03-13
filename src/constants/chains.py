class Chains:
    all_chains = {
        "0x1": {
            "id": "0x1",
            "name": "Ethereum",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FETH.png?alt=media&token=55db834b-029b-4237-9b30-f5fd28d7b2f4",
            "explorerUrl": "https://etherscan.io/",
            "type": "tx",
        },
        "0x38": {
            "id": "0x38",
            "name": "BNB Chain",
            "nativeTokenId": "binancecoin",
            "nativeTokenSymbol": "BNB",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FBNB.png?alt=media&token=b0a77aea-6f98-4916-9dbf-ffdc9b44c2c3",
            "explorerUrl": "https://bscscan.com/",
            "type": "tx",
        },
        "0x89": {
            "id": "0x89",
            "name": "Polygon",
            "nativeTokenId": "matic-network",
            "nativeTokenSymbol": "MATIC",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FMATIC.png?alt=media&token=f3dd80ba-b045-40ba-9c8c-ee0d9617d798",
            "explorerUrl": "https://polygonscan.com/",
            "type": "tx",
        },
        "0xfa": {
            "id": "0xfa",
            "name": "Fantom",
            "nativeTokenId": "fantom",
            "nativeTokenSymbol": "FTM",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FFTM.png?alt=media&token=0fc3758c-9aa3-491b-904b-46fabb097447",
            "explorerUrl": "https://ftmscan.com/",
            "type": "tx",
        },
        "0xa4b1": {
            "id": "0xa4b1",
            "name": "Arbitrum",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Farbitrum.jpeg?alt=media&token=cd5a7393-1488-4d3a-8eeb-f9b7d65d952b",
            "explorerUrl": "https://arbiscan.io/",
            "type": "tx",
        },
        "0xa": {
            "id": "0xa",
            "name": "Optimism",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Foptimism.png?alt=media&token=5bdb5bd7-6aa7-4c31-bc49-121e869f6b49",
            "explorerUrl": "https://optimistic.etherscan.io/",
            "type": "tx",
        },
        "0xa86a": {
            "id": "0xa86a",
            "name": "Avalanche C-Chain",
            "nativeTokenId": "avalanche-2",
            "nativeTokenSymbol": "AVAX",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX.png?alt=media&token=1e01b02f-0fb2-4887-b84d-837a4e2880dd",
            "explorerUrl": "https://43114.snowtrace.io/",
            "type": "tx",
        },
        "x-avax": {
            "id": "x-avax",
            "name": "Avalanche X-Chain",
            "nativeTokenId": "avalanche-2",
            "nativeTokenSymbol": "AVAX",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX-X.png?alt=media&token=ebaa0fc0-3420-42aa-ad0b-39bdeea3c074",
            "explorerUrl": "https://avascan.info/blockchain/x/",
            "type": "tx",
        },
        "p-avax": {
            "id": "p-avax",
            "name": "Avalanche P-Chain",
            "nativeTokenId": "avalanche-2",
            "nativeTokenSymbol": "AVAX",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FAVAX-P.png?alt=media&token=3d8caa29-28e8-46a0-8248-393637b8589d",
            "explorerUrl": "https://avascan.info/blockchain/p/",
            "type": "tx",
        },
        "0x2b6653dc": {
            "id": "0x2b6653dc",
            "name": "Tron",
            "nativeTokenId": "tron",
            "nativeTokenSymbol": "TRX",
            "nativeTokenDecimals": 6,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2FTRX.png?alt=media&token=85e1e5a3-26bc-433b-81dd-f2733c7ffe80",
            "explorerUrl": "https://tronscan.org/",
            "type": "transaction",
        },
        "0x19": {
            "id": "0x19",
            "name": "Cronos",
            "nativeTokenId": "crypto-com-chain",
            "nativeTokenSymbol": "CRO",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://assets.coingecko.com/coins/images/7310/large/cro_token_logo.png",
            "explorerUrl": "https://cronoscan.com/",
            "type": "tx",
        },
        "solana": {
            "id": "solana",
            "name": "Solana",
            "nativeTokenId": "solana",
            "nativeTokenSymbol": "SOL",
            "nativeTokenDecimals": 9,
            "imgUrl": "https://assets.coingecko.com/coins/images/4128/large/solana.png",
            "explorerUrl": "https://solscan.io/",
            "type": "tx",
        },
        "polkadot": {
            "id": "polkadot",
            "name": "Polkadot",
            "nativeTokenId": "polkadot",
            "nativeTokenSymbol": "DOT",
            "nativeTokenDecimals": 10,
            "imgUrl": "https://assets.coingecko.com/coins/images/12171/large/polkadot.png",
            "explorerUrl": "https://polkadot.subscan.io/",
            "type": "extrinsic",
        },
        "0x2105": {
            "id": "0x2105",
            "name": "Base",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/31199/large/59302ba8-022e-45a4-8d00-e29fe2ee768c-removebg-preview.png",
            "explorerUrl": "https://basescan.org/",
            "type": "tx",
        },
        "0x8ae": {
            "id": "0x8ae",
            "name": "Kava",
            "nativeTokenId": "kava",
            "nativeTokenSymbol": "KAVA",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/9761/large/kava.png",
            "explorerUrl": "https://kavascan.io/",
            "type": "tx",
        },
        "0x64": {
            "id": "0x64",
            "name": "Gnosis",
            "nativeTokenId": "xdai",
            "nativeTokenSymbol": "XDAI",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/11062/large/Identity-Primary-DarkBG.png",
            "explorerUrl": "https://gnosisscan.io/",
            "type": "tx",
        },
        "0x2019": {
            "id": "0x2019",
            "name": "Kaia",
            "nativeTokenId": "klay-token",
            "nativeTokenSymbol": "KLAY",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/9672/large/klaytn.png",
            "explorerUrl": "https://klaytnscope.com/",
            "type": "tx",
        },
        "0x1388": {
            "id": "0x1388",
            "name": "Mantle",
            "nativeTokenId": "mantle",
            "nativeTokenSymbol": "MNT",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/30980/large/token-logo.png",
            "explorerUrl": "https://mantlescan.info/",
            "type": "tx",
        },
        "0xa4ec": {
            "id": "0xa4ec",
            "name": "Celo",
            "nativeTokenId": "celo",
            "nativeTokenSymbol": "CELO",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/11090/large/InjXBNx9_400x400.jpg",
            "explorerUrl": "https://celoscan.io/",
            "type": "tx",
        },
        "0x504": {
            "id": "0x504",
            "name": "Moonbeam",
            "nativeTokenId": "moonbeam",
            "nativeTokenSymbol": "GLMR",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/22459/large/Moonbeam_GLMR_ICON.png",
            "explorerUrl": "https://moonscan.io/",
            "type": "tx",
        },
        "0xa9": {
            "id": "0xa9",
            "name": "Manta",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/34289/large/manta.jpg",
            "explorerUrl": "https://manta.socialscan.io/",
            "type": "tx",
        },
        "0x171": {
            "id": "0x171",
            "name": "PulseChain",
            "nativeTokenId": "pulsechain",
            "nativeTokenSymbol": "PLS",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/25666/large/PLS-LogoTransparent_1.png",
            "explorerUrl": "https://scan.pulsechainfoundation.org/#/",
            "type": "tx",
        },
        "0x1e": {
            "id": "0x1e",
            "name": "Rootstock",
            "nativeTokenId": "rootstock",
            "nativeTokenSymbol": "RBTC",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/5070/large/RBTC-logo.png",
            "explorerUrl": "https://explorer.rootstock.io/",
            "type": "tx",
        },
        "0x250": {
            "id": "0x250",
            "name": "Astar",
            "nativeTokenId": "astar",
            "nativeTokenSymbol": "ASTR",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/22617/large/astr.png",
            "explorerUrl": "https://astar.blockscout.com/",
            "type": "tx",
        },
        "0x440": {
            "id": "0x440",
            "name": "Metis",
            "nativeTokenId": "metis-token",
            "nativeTokenSymbol": "METIS",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/15595/large/Metis_Black_Bg.png",
            "explorerUrl": "https://explorer.metis.io/",
            "type": "tx",
        },
        "0x1e14": {
            "id": "0x1e14",
            "name": "Canto",
            "nativeTokenId": "canto",
            "nativeTokenSymbol": "CANTO",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/26959/large/canto-network.png",
            "explorerUrl": "https://cantoscan.com/",
            "type": "tx",
        },
        "0x80": {
            "id": "0x80",
            "name": "Huobi ECO",
            "nativeTokenId": "huobi-token",
            "nativeTokenSymbol": "HT",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/2822/large/huobi-token-logo.png",
            "explorerUrl": "https://hecoscan.io/#/",
            "type": "transaction",
        },
        "0xe708": {
            "id": "0xe708",
            "name": "Linea",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://lineascan.build/assets/linea/images/svg/logos/chain-dark.svg",
            "explorerUrl": "https://lineascan.build/",
            "type": "tx",
        },
        "0x42": {
            "id": "0x42",
            "name": "OKT Chain",
            "nativeTokenId": "oec-token",
            "nativeTokenSymbol": "OKT",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/13708/large/WeChat_Image_20220118095654.png",
            "explorerUrl": "https://www.okx.com/web3/explorer/oktc/",
            "type": "tx",
        },
        "0x4e454152": {
            "id": "0x4e454152",
            "name": "Aurora",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/20582/large/aurora.jpeg",
            "explorerUrl": "https://explorer.mainnet.aurora.dev/",
            "type": "tx",
        },
        "0x505": {
            "id": "0x505",
            "name": "Moonriver",
            "nativeTokenId": "moonriver",
            "nativeTokenSymbol": "MOVR",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/17984/large/Moonriver_MOVR_ICON.png",
            "explorerUrl": "https://moonriver.moonscan.io/",
            "type": "tx",
        },
        "0x5afe": {
            "id": "0x5afe",
            "name": "Oasis Sapphire",
            "nativeTokenId": "oasis-network",
            "nativeTokenSymbol": "ROSE",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/13162/large/rose.png",
            "explorerUrl": "https://explorer.oasis.io/mainnet/sapphire/",
            "type": "tx",
        },
        "0xee": {
            "id": "0xee",
            "name": "Blast",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/35494/large/Blast.jpg",
            "explorerUrl": "https://blastscan.io/",
            "type": "tx",
        },
        "orai": {
            "id": "orai",
            "name": "Oraichain",
            "nativeTokenId": "oraichain-token",
            "nativeTokenSymbol": "ORAI",
            "nativeTokenDecimals": 6,
            "imgUrl": "https://firebasestorage.googleapis.com/v0/b/token-c515a.appspot.com/o/tokens_v2%2Foraichain.png?alt=media&token=9e13f006-8674-4bfb-a1fc-4672c37944f7",
            "explorerUrl": "https://scan.orai.io/",
            "type": "txs",
        },
        "cosmos": {
            "id": "cosmos",
            "name": "Cosmos Hub",
            "nativeTokenId": "cosmos",
            "nativeTokenSymbol": "ATOM",
            "nativeTokenDecimals": 6,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/1481/large/cosmos_hub.png",
            "explorerUrl": "https://www.mintscan.io/cosmos/",
            "type": "tx",
        },
        "ton": {
            "id": "ton",
            "name": "The Open Network",
            "nativeTokenId": "the-open-network",
            "nativeTokenSymbol": "TON",
            "nativeTokenDecimals": 9,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/17980/large/photo_2024-09-10_17.09.00.jpeg",
            "explorerUrl": "https://tonscan.org/",
            "type": "tx",
        },
        "0x1b58": {
            "id": "0x1b58",
            "name": "ZetaChain",
            "nativeTokenId": "zetachain",
            "nativeTokenSymbol": "ZETA",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://coin-images.coingecko.com/coins/images/26718/large/Twitter_icon.png",
            "explorerUrl": "https://explorer.zetachain.com/",
            "type": "tx",
        },
        "0x144": {
            "id": "0x144",
            "name": "ZkSync",
            "nativeTokenId": "ethereum",
            "nativeTokenSymbol": "ETH",
            "nativeTokenDecimals": 18,
            "imgUrl": "https://s2.coinmarketcap.com/static/img/coins/128x128/24091.png",
            "explorerUrl": "https://explorer.zksync.io/",
            "type": "tx",
        },
    }

    mapping = {
        "bsc": "0x38",
        "ethereum": "0x1",
        "ftm": "0xfa",
        "polygon": "0x89",
        "arbitrum": "0xa4b1",
        "optimism": "0xa",
        "avalanche": "0xa86a",
        "tron": "0x2b6653dc",
        "base": "0x2105",
    }

    reverse_mapping = {
        "0x38": "bsc",
        "0x1": "ethereum",
        "0x89": "polygon",
        "0xfa": "ftm",
        "0xa4b1": "arbitrum",
        "0xa": "optimism",
        "0xa86a": "avalanche",
        "0x2b6653dc": "tron",
        "0x2105": "base",
    }
