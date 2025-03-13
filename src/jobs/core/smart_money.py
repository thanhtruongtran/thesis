import calendar
import time

import pymongo
from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cache import MongoDBCache
from src.databases.mongodb_etl_v2 import MongoDBETL
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_trader import MongoDBTrader
from src.models.transaction import Transaction, merge_transactions, split_transactions
from src.utils.logger import get_logger

logger = get_logger("Update Most Profitable Trader Job")


class UpdateProfitableTraderJob(SchedulerJob):
    def __init__(
        self,
        time_interval: int,
        interval,
        delay,
        run_now,
        chain_id: str,
        db_prefix: str,
    ):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler, retry=False)
        self.time_interval = time_interval
        self.chain_id = chain_id

        # Market Trading Tracker DB
        self.mongodb_trader = MongoDBTrader()

        # Mongo DB ETL
        self.mongodb_etl = MongoDBETL(db_prefix=db_prefix)

        # Mongo DB Centic Cache
        self.mongodb_cache = MongoDBCache()

        # Mongo DB KLG
        self.mongodb_klg = MongoDBKLG()

    def _pre_start(self):
        self.pair_addresses = {}

        # Time Interval to TimeFrame
        time_interval_to_timeframe = {"1": "1D", "3": "3D", "7": "1W", "30": "1M"}
        self.timeframe = time_interval_to_timeframe[str(self.time_interval)]
        logger.info(f"Time Frame: {self.timeframe}")

        # Variables for running
        self.contract_errs = {}
        self.contract_info = {}
        self.token_info = {}
        self.contract_errs = {}
        self.wallet_transactions = {}
        self.wallet_rs = {}

    def _start(self):
        self.midnight_timestamp = get_midnight_gmt_timestamp()
        before_timestamp = self.midnight_timestamp - 86400 * self.time_interval
        before_block_number = self.mongodb_etl.get_block_number_from_timestamp(
            before_timestamp
        )
        current_block_number = self.mongodb_etl.get_block_number_from_timestamp(
            self.midnight_timestamp
        )
        logger.info(f"Block number: {before_block_number}  - {current_block_number}")

        for i in range(before_block_number, current_block_number, 10000):
            start_block = i
            end_block = min(current_block_number, i + 9999)
            self.process_event(start_block, end_block)

        logger.info(f"Total contract with errs: {len(self.contract_errs)}")

        self.wallet_transactions = {
            key: value
            for key, value in self.wallet_transactions.items()
            if len(value) <= 30
        }

    def _execute(self, *args, **kwargs):
        if self.time_interval == 30:
            self.mongodb_trader.update_pair_addresses(
                self.chain_id, self.pair_addresses
            )
        logger.info("Executing result")
        for wallet, transactions in self.wallet_transactions.items():
            self.wallet_rs[wallet] = {}
            risk_appetite = {
                "Extremely Risk": 0,  # <$1M
                "High Risk": 0,  # 1M-10M
                "Risk": 0,  # 10-100M
                "Mid Cap": 0,  # 100-500M
                "Large Cap": 0,
            }
            trading_volume = 0
            price_current_list = []
            price_token0_list = []
            last_trading_timestamp = -1
            txs = {}

            for transaction in transactions:
                txs[str(transaction.trading_time)] = {
                    "transactionHash": transaction.transaction_hash,
                    "fromAmount": transaction.from_amount,
                    "fromToken": transaction.from_token,
                    "toAmount": transaction.to_amount,
                    "toToken": transaction.to_token,
                    # f"{transaction.from_amount} {transaction.from_token} -> {transaction.to_amount} {transaction.to_token}"
                }

                last_trading_timestamp = max(
                    last_trading_timestamp, transaction.trading_time
                )

                price_current_list.append(
                    self.token_info[transaction.from_token]["price"]
                    / self.token_info[transaction.to_token]["price"]
                )
                price_token0_list.append(
                    self.token_info[transaction.from_token]["price"]
                )
                addr0 = transaction.from_token
                addr1 = transaction.to_token

                if addr0 in self.contract_errs or addr1 in self.contract_errs:
                    continue

                volume = self.token_info[addr0]["price"] * transaction.from_amount
                trading_volume += volume

                min_cap = min(
                    self.token_info[addr0]["marketCap"],
                    self.token_info[addr1]["marketCap"],
                )
                if min_cap < 1000000:
                    risk_appetite["Extremely Risk"] += volume
                elif min_cap < 10000000:
                    risk_appetite["High Risk"] += volume
                elif min_cap < 100000000:
                    risk_appetite["Risk"] += volume
                elif min_cap < 500000000:
                    risk_appetite["Mid Cap"] += volume
                else:
                    risk_appetite["Large Cap"] += volume

            try:
                merged_txs = merge_transactions(split_transactions(transactions))
            except Exception as e:
                logger.info(f"{e} at wallet {wallet}")
                continue

            self.wallet_rs[wallet].update(
                {
                    "PnL": self.cal_pnl(merged_txs),
                    "tradingVolume": trading_volume,
                    "riskAppetite": max(risk_appetite, key=risk_appetite.get),
                    "transactions": txs,
                    "lastTradingTimestamp": last_trading_timestamp,
                }
            )

    def _end(self):
        # Delete old docs filtered by time interval
        logger.info(f"Deleting old {self.timeframe} traders list")
        self.mongodb_trader.clear_traders_with_timeframe(self.chain_id, self.timeframe)

        logger.info("Updating to db")
        # Bulk write wallet results
        bulk_writes = []
        for wallet, result in self.wallet_rs.items():
            result["timeFrame"] = self.timeframe
            result["chainId"] = self.chain_id
            result["wallet"] = wallet
            bulk_writes.append(
                pymongo.UpdateOne(
                    {"_id": f"{self.chain_id}_{self.timeframe}_{wallet}"},
                    {"$set": result},
                    upsert=True,
                )
            )
        self.mongodb_trader.bulk_write_collection_trader(self.timeframe, bulk_writes)
        self.mongodb_trader.update_config(
            stream_id=f"profitable_traders_{self.timeframe}_{self.chain_id}",
            timestamp=self.midnight_timestamp,
        )

        # end_timestamp = time.time()
        # logger.info(f"Job took {end_timestamp - self.current_timestamp}s")

    def cal_pnl(self, transactions: list[Transaction]):
        pnl_in_USD = 0
        N = len(transactions)
        for i in range(N):
            transaction = transactions[i]
            from_addr = transaction.from_token
            to_addr = transaction.to_token
            if from_addr in self.contract_errs or to_addr in self.contract_errs:
                continue
            price_token0 = self.token_info[from_addr]["price"]
            price_token1 = self.token_info[to_addr]["price"]
            pnl_in_USD += (
                price_token1 * transaction.to_amount
                - price_token0 * transaction.from_amount
            )

        return pnl_in_USD

    def process_event(self, start_block, end_block):
        logger.info(f"Taking events from block {start_block} to {end_block}")
        docs = self.mongodb_etl.get_dex_events_list(
            start_block=start_block, end_block=end_block
        )

        logger.info(f"Take events Done. Total events:  {len(docs)}")

        count = 0

        for doc in docs:
            if (
                "wallet" not in doc
                or "contract_address" not in doc
                or "transaction_hash" not in doc
            ):
                continue
            if "amount0" not in doc and "amount0_in" not in doc:
                continue

            wallet = doc["wallet"]
            contract_address = doc["contract_address"]
            transaction_hash = doc["transaction_hash"]

            if contract_address in self.contract_errs:
                continue
            block_timestamp = doc["block_timestamp"]

            if "amount0_in" in doc:
                amount0_in = int(doc["amount0_in"])
                amount0_out = int(doc["amount0_out"])
                amount1_in = int(doc["amount1_in"])
                amount1_out = int(doc["amount1_out"])

                # Find contract pair info
                if contract_address not in self.contract_info:
                    self.contract_info[contract_address] = (
                        self.mongodb_cache.get_pairs_info(
                            self.chain_id, contract_address
                        )
                    )

                if amount0_in == 0:
                    token1_info, token0_info, pair_addr = self.contract_info[
                        contract_address
                    ]
                    amt0 = amount1_in
                    amt1 = amount0_out
                else:
                    token0_info, token1_info, pair_addr = self.contract_info[
                        contract_address
                    ]
                    amt0 = amount0_in
                    amt1 = amount1_out

            else:
                amount0 = convert_large_number_to_integer_with_sign(int(doc["amount0"]))
                amount1 = convert_large_number_to_integer_with_sign(int(doc["amount1"]))

                # Find contract pair info
                if contract_address not in self.contract_info:
                    self.contract_info[contract_address] = (
                        self.mongodb_cache.get_pairs_info(
                            self.chain_id, contract_address
                        )
                    )
                if amount0 < 0:
                    token1_info, token0_info, pair_addr = self.contract_info[
                        contract_address
                    ]
                    amt0 = amount1
                    amt1 = abs(amount0)
                else:
                    token0_info, token1_info, pair_addr = self.contract_info[
                        contract_address
                    ]
                    amt0 = amount0
                    amt1 = abs(amount1)

            # Check and add token info (address, symbol, decimals, price)
            if token0_info is None or token1_info is None:
                continue
            self.pair_addresses[pair_addr] = 1
            if token0_info["address"] not in self.token_info:
                decimals0, symbol0, marketCap0 = self.mongodb_klg.get_token_information(
                    self.chain_id, token0_info["address"]
                )
                price0 = self.mongodb_cache.get_current_price(
                    self.chain_id, token0_info["address"]
                )
                if "symbol" in token0_info:
                    symbol0 = symbol0 or token0_info["symbol"]
                if (
                    symbol0 is None
                    or price0 is None
                    or decimals0 is None
                    or marketCap0 is None
                ):
                    self.contract_errs[contract_address] = 1
                    continue
                if "decimals" in token0_info:
                    decimals0 = token0_info["decimals"]
                self.token_info[token0_info["address"]] = {
                    "address": token0_info["address"],
                    "symbol": symbol0,
                    "decimals": decimals0,
                    "price": price0,
                    "marketCap": marketCap0,
                }
            if token1_info["address"] not in self.token_info:
                decimals1, symbol1, marketCap1 = self.mongodb_klg.get_token_information(
                    self.chain_id, token1_info["address"]
                )
                price1 = self.mongodb_cache.get_current_price(
                    self.chain_id, token1_info["address"]
                )
                if "symbol" in token1_info:
                    symbol1 = token1_info["symbol"]
                if (
                    symbol1 is None
                    or price1 is None
                    or decimals1 is None
                    or marketCap1 is None
                ):
                    self.contract_errs[contract_address] = 1
                    continue
                if "decimals" in token1_info:
                    decimals1 = token1_info["decimals"]
                self.token_info[token1_info["address"]] = {
                    "address": token1_info["address"],
                    "symbol": symbol1,
                    "decimals": decimals1,
                    "price": price1,
                    "marketCap": marketCap1,
                }

            # Add wallet txs
            if wallet not in self.wallet_transactions:
                self.wallet_transactions[wallet] = []
            self.wallet_transactions[wallet].append(
                Transaction(
                    amt0 / 10 ** self.token_info[token0_info["address"]]["decimals"],
                    token0_info["address"],
                    amt1 / 10 ** self.token_info[token1_info["address"]]["decimals"],
                    token1_info["address"],
                    block_timestamp,
                    transaction_hash,
                )
            )
            count += 1
            if count % 5000 == 0:
                logger.info(f"Queried {count} txs")


def convert_large_number_to_integer_with_sign(large_number, size=256):
    sign = 1 << size - 1
    number_int256 = (large_number & sign - 1) - (large_number & sign)
    return number_int256


def get_midnight_gmt_timestamp():
    now = time.gmtime()
    midnight = (now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0)
    return calendar.timegm(midnight)
