from src.databases.mongodb_klg import MongoDBKLG


class Entity:
    token = "TOKEN"
    project = "PROJECT"
    chain = "CHAIN"
    wallet = "WALLET"
    event = "EVENT"
    trend = "TREND"


class cfg_entity:
    def __init__(self):
        self.mongodb = MongoDBKLG()
        cursor = self.mongodb._db["smart_contracts"].find(
            {
                "idCoinGecko": {"$exists": True},
            },
            {"symbol": 1, "name": 1}
        )
        tokens = []
        for token in cursor:
            tokens.append(token["symbol"])
            tokens.append(token["name"])

        chain = [
            "BSC", "BNB Smart Chain Ecosystem",
            "Ethereum", "Ethereum Ecosystem",
            "Solana", "Solana Ecosystem",
            "Berachain", "Berachain Ecosystem",
            "Polygon", "Polygon Ecosystem",
            "Avalanche", "Avalanche Ecosystem", 
            "Tron", "Tron Ecosystem",
            "Arbitrum", "Arbitrum Ecosystem",
            "Cronos", "Cronos Ecosystem",
            "Cosmos", "Cosmos Ecosystem",
            "Polkadot", "Polkadot Ecosystem",
            "Base", "Base Ecosystem",
            "TON", "TON Ecosystem",
            "ZkSync", "ZkSync Ecosystem",
            "Optimism", "Optimism Ecosystem",
            "Oraichain", "Oraichain Ecosystem",
        ]

        trend = [
            "Stablecoin", "Defi", "Real World Assets", "RWA"
            "Memescoin", "Meme", "AI", "Depin", "NFT", "GameFi", 
            "Metaverse", "DAO", "SocialFi", "Web3", "Layer 1", "Layer 2",
            "Crosschain", "Interoperability", "Privacy", "Security", 
            "Scalability", "Sustainability", "Regulation", "Compliance",
            "DEX", "Infrastructure", "Lending", "Borrowing", "Insurance",
            "AI Agents", "Staking", "Yield Farming", "Swaps"
        ]
        
        self.tokens = tokens
        self.chain = chain
        self.trend = trend
