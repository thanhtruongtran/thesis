class Operator:
    exists = "$exists"
    in_ = "$in"
    regex = "$regex"
    options = "$options"


class MongoDBCollections:
    wallets = "wallets"
    projects = "projects"
    ecosystems = "ecosystems"
    smart_contracts = "smart_contracts"
    profiles = "profiles"
    users = "users"
    notifications = "notifications"
    abi = "abi"
    configs = "configs"
    multichain_wallets = "multichain_wallets"
    interactions = "interactions"
    cex_interactions = "cex_interactions"
    multichain_wallets_credit_scores = "multichain_wallets_credit_scores_v3"

    is_part_ofs = "is_part_ofs"
    transfers = "transfers"
    deposits = "deposits"
    withdraws = "withdraws"
    borrows = "borrows"
    repays = "repays"
    liquidates = "liquidates"
    followers = "followers"
    blocks = "blocks"


class MongoDBDexCollections:
    configs = "configs"

    dexes = "dexes"
    pairs = "pairs"
    cexes = "cexes"
    wallets = "wallets"
    tokens = "tokens"
    nfts = "nfts"
    lendings = "lendings"
    tickers = "tickers"
    exchanges = "exchanges"
    token_prices = "token_prices"
    token_prices_draft = 'token_prices_draft'


class MongoDBContractLabelCollections:
    mapped_projects = "mapped_projects"
    protocols = "protocols"
    smart_contracts = "smart_contracts"


class MongoDBLabelCollections:
    smart_contracts = "smartContracts"
    wallets = "wallets"
    labels = "labels"


class MongoDBCommunityCollections:
    deposit_users = "depositUsers"
    user_deposits = "userDeposits"


class MongoDBdYdXCollections:
    wallets = "wallets"
    smart_contracts = "smart_contracts"
    events = "events"
    configs = "configs"


class MongoDBAuthCollections:
    apigee_keys = "centic_api_apigee_keys"
    requests_count = "centic_api_key_requests_count"
    apis = "centic_api_apis"
    plans = "centic_api_plans"
    request_units_count = "centic_api_request_units_count"


class MongoDBPageRankCollections:
    wallets = "wallet_addresses"


class MongoDBCDPCollections:
    configs = "configs"
    actions = "actions"
    actions_v2 = "actions_v2"
    web2_actions = "web2_actions"
    web2_actions_v2 = "web2_actions_v2"
    web2_events = "web2_events"
    web2_events_v2 = "web2_events_v2"
    users = "users"
    users_v2 = "users_v2"
    wallets = "wallets"
    wallets_v2 = "wallets_v2"
    events = "events"
    events_v2 = "events_v2"
    twitter_users = "twitter_users"
    twitter_tweets = "tweets"
    twitter_follows = "twitter_follows"
    telegram_users = "telegram_users"
    telegram_messages = "telegram_messages"
    discord_servers = "discord_guilds"
    discord_members = "discord_users"
    discord_messages = "discord_messages"
    discord_channels = "discord_channels"
    centic_tasks = "centic_tasks"
    centic_claimed_tasks = "centic_claimed_tasks"
    centic_tasks_users = "centic_tasks_users"


class DexNFTManagerCollections:
    wallets = "wallets"
    configs = "configs"
    collectors = "collectors"
    dex_nfts = "dex_nft"
    dex_events_etl = "dex_events"
    pairs = "pairs"


class MongoDBEtlCollections:
    dex_events = 'dex_events'
    transactions = 'transactions'
    blocks = 'blocks'


class MongoDBCacheCollections:
    pairs = 'pairs'
    token_prices = 'token_prices'


class MongoDBWeb3TrackerCollections:
    web3_traders = 'web3_traders'
    configs = 'configs'
    potential_tokens = 'potential_tokens'
    potential_tokens_log = 'potential_tokens_log'
    backtest = 'backtest'


class MongoDBWeb3TrackerCollectionsForBacktest:
    web3_traders = 'z_web3_traders'
    web3_traders_cex = 'z_web3_traders_cex'
    configs = 'zt_configs'
    potential_tokens = 'zt_potential_tokens'
    cex_potential_tokens = 'zt_cex_potential_tokens'
    backtest = 'zt_backtest'
