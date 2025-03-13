import datetime
import sys

import pymongo
from pymongo import DeleteMany, MongoClient

from src.constants.config import MongoDBTraderConfig

# from src.artifacts.binance_0x1_tokens import binance_0x1_tokens
from src.constants.mongodb import (
    MongoDBWeb3TrackerCollections,
    MongoDBWeb3TrackerCollectionsForBacktest,
)
from src.utils.logger import get_logger

logger = get_logger("MongoDB Trader")


class MongoDBTrader:
    def __init__(
        self, connection_url=MongoDBTraderConfig.CONNECTION_URL, database=MongoDBTraderConfig.DATABASE, is_backtest=False
    ):
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to MongoDB: {connection_url}: {e}")
            sys.exit(1)

        if not is_backtest:
            self._web3_traders_col_name = MongoDBWeb3TrackerCollections.web3_traders
            self._configs_col = self.mongo_db[MongoDBWeb3TrackerCollections.configs]
            self._potential_tokens_col = self.mongo_db[
                MongoDBWeb3TrackerCollections.potential_tokens
            ]
            self._potential_tokens_log_col = self.mongo_db[
                MongoDBWeb3TrackerCollections.potential_tokens_log
            ]
            self._backtest_col = self.mongo_db[MongoDBWeb3TrackerCollections.backtest]
        else:
            self._web3_traders_col_name = (
                MongoDBWeb3TrackerCollectionsForBacktest.web3_traders
            )
            self._configs_col = self.mongo_db[
                MongoDBWeb3TrackerCollectionsForBacktest.configs
            ]
            self._potential_tokens_col = self.mongo_db[
                MongoDBWeb3TrackerCollectionsForBacktest.potential_tokens
            ]
            self._cex_potential_tokens_col = self.mongo_db[
                MongoDBWeb3TrackerCollectionsForBacktest.cex_potential_tokens
            ]
            self._backtest_col = self.mongo_db[
                MongoDBWeb3TrackerCollectionsForBacktest.backtest
            ]
            self._web3_traders_col_name_cex = (
                MongoDBWeb3TrackerCollectionsForBacktest.web3_traders_cex
            )

    def get_top_traders_by_timeframe(self, chain_id, timeframe, percentage):
        logger.info(
            f"Get {percentage}% top traders in {timeframe} timeframe of chain ID {chain_id}"
        )
        col_name = f"{self._web3_traders_col_name}_{timeframe}"
        collection = self.mongo_db[col_name]
        total_doc_in_collection = collection.count_documents({"chainId": chain_id})
        doc_count = int(total_doc_in_collection / 100 * percentage)

        cursor = (
            collection.find({"chainId": chain_id}).sort([("PnL", -1)]).limit(doc_count)
        )
        docs = list(cursor)
        return docs

    def backtest_get_top_traders_by_timeframe(
        self, chain_id, hour_start, timeframe, percentage, is_cex=False
    ):
        query = {"hourStart": hour_start, "chainId": chain_id}
        logger.info(
            f"Get {percentage}% top traders, Hour start = {hour_start}, in {timeframe} timeframe of chain ID {chain_id}"
        )
        if not is_cex:
            col_name = f"{self._web3_traders_col_name}_{timeframe}"
        else:
            col_name = f"{self._web3_traders_col_name_cex}_{timeframe}"
        collection = self.mongo_db[col_name]
        total_doc_in_collection = collection.count_documents(query)
        doc_count = int(total_doc_in_collection / 100 * percentage)

        cursor = collection.find(query).sort({"PnL": -1}).limit(doc_count)
        docs = list(cursor)
        return docs

    def bulk_write_collection_trader(self, timeframe: str, bulk_writes: list):
        col_name = f"{self._web3_traders_col_name}_{timeframe}"
        collection = self.mongo_db[col_name]
        logger.info("Bulk write to DB")
        collection.bulk_write(bulk_writes)

    def bulk_write_collection_trader_cex(self, timeframe: str, bulk_writes: list):
        col_name = f"{self._web3_traders_col_name_cex}_{timeframe}"
        collection = self.mongo_db[col_name]
        logger.info("Bulk write to DB")
        collection.bulk_write(bulk_writes)

    def clear_traders_with_timeframe(self, chain_id, timeframe: str):
        col_name = f"{self._web3_traders_col_name}_{timeframe}"
        collection = self.mongo_db[col_name]
        operations = [DeleteMany({"chainId": chain_id})]
        logger.info("Clear old docs in DB")
        collection.bulk_write(operations)

    def clear_potential_tokens(self, chain_id):
        operations = [DeleteMany({"chainId": chain_id})]
        self._potential_tokens_col.bulk_write(operations)

    def update_config(self, stream_id, timestamp):
        logger.info("Update config")
        self._configs_col.update_one(
            {"_id": stream_id}, {"$set": {"lastUpdatedAt": timestamp}}, upsert=True
        )

    def update_top_tokens(self, chain_id, data_dict: dict):
        logger.info(f"Delete old Top Potential Tokens data, chain ID {chain_id}")
        operations = [DeleteMany({"chainId": chain_id})]
        self._potential_tokens_col.bulk_write(operations)

        bulks = []
        rank = 1

        logger.info(f"Updating new Top Potential Tokens data, chain ID {chain_id}")
        for token_addr, data in data_dict.items():
            data["chainId"] = chain_id
            data["tokenAddress"] = token_addr
            data["rank"] = rank
            data["date"] = datetime.datetime.now().strftime("%d-%m-%Y")
            rank += 1
            marketCap = data["marketCap"]
            if marketCap < 1000000:
                risk_appetite = "Extremely Risk"
            elif marketCap < 10000000:
                risk_appetite = "High Risk"
            elif marketCap < 100000000:
                risk_appetite = "Risk"
            elif marketCap < 500000000:
                risk_appetite = "Mid Cap"
            else:
                risk_appetite = "Large Cap"
            data["riskAppetite"] = risk_appetite
            data["amountBackedTopTraders"] = len(data["backedBy"])
            _id = f"{chain_id}_{token_addr}"
            bulks.append(pymongo.UpdateOne({"_id": _id}, {"$set": data}, upsert=True))
        self._potential_tokens_col.bulk_write(bulks)
        self._potential_tokens_log_col.bulk_write(bulks)

    def backtest_update_top_tokens(
        self, chain_id, data_dict: dict, hour_start, timestamp
    ):
        bulks = []

        logger.info(f"Updating new Top Potential Tokens data, chain ID {chain_id}")
        for token_addr, data in data_dict.items():
            data["chainId"] = chain_id
            data["tokenAddress"] = token_addr
            data["hourStart"] = hour_start
            data["amountBackedTopTraders"] = len(data["backedBy"])
            tmp_dt_object = datetime.datetime.fromtimestamp(
                timestamp + hour_start * 3600
            )
            _id = f"{chain_id}_{hour_start}_{token_addr}"
            bulks.append(
                pymongo.UpdateOne(
                    {"_id": _id},
                    {"$set": {**data, "date": tmp_dt_object.strftime("%d-%m-%Y")}},
                    upsert=True,
                )
            )
        if len(bulks) > 0:
            self._potential_tokens_col.bulk_write(bulks)

    def update_pair_addresses(self, chain_id, data_dict: dict):
        logger.info("Update pair addresses")

        _id = f"{chain_id}_pair_addresses"
        pairs = list(data_dict.keys())
        self._configs_col.update_one(
            {"_id": _id},
            {
                "$set": {
                    "chainId": chain_id,
                    "numberOfPairs": len(pairs),
                    "pairs": pairs,
                }
            },
            upsert=True,
        )
