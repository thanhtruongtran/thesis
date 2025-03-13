import sys

from pymongo import MongoClient

from src.constants.config import MongoDBETLConfig
from src.constants.mongodb import MongoDBEtlCollections
from src.utils.logger import get_logger

logger = get_logger("MongoDB ETL")


class MongoDBETL:
    def __init__(
        self,
        db_prefix,
        connection_url=MongoDBETLConfig.CONNECTION_URL,
        database=MongoDBETLConfig.DATABASE,
    ):
        try:
            self.connection = MongoClient(connection_url)
            database_name = db_prefix + database
            self.mongo_db_etl = self.connection[database_name]
        except Exception as e:
            logger.exception(f"Failed to connect to MongoDB: {connection_url}: {e}")
            sys.exit(1)

        self._dex_events_col = self.mongo_db_etl[MongoDBEtlCollections.dex_events]
        self._transactions_col = self.mongo_db_etl[MongoDBEtlCollections.transactions]
        self._blocks_col = self.mongo_db_etl[MongoDBEtlCollections.blocks]

    def get_block_number_from_timestamp(self, timestamp):
        logger.info(f"Get block number of timestamp {timestamp}")
        cursor_transaction = (
            self._transactions_col.find({"block_timestamp": {"$gte": timestamp}})
            .sort("block_timestamp", 1)
            .limit(1)
        )
        return cursor_transaction[0]["block_number"]

    def get_block_number_from_timestamp_by_dex_events_collection(
        self, target_timestamp
    ):
        # logger.info(f"Get block number of timestamp {target_timestamp}")

        latest_block = (
            self._dex_events_col.find({})
            .sort({"block_number": -1})
            .limit(1)[0]["block_number"]
        )
        earliest_block = 18037987

        # Perform binary search
        while earliest_block <= latest_block:
            middle_block = (earliest_block + latest_block) // 2
            middle_block_timestamp = (
                self._dex_events_col.find({"block_number": {"$gte": middle_block}})
                .sort({"block_number": 1})
                .limit(1)[0]["block_timestamp"]
            )

            if middle_block_timestamp < target_timestamp:
                earliest_block = middle_block + 1
            elif middle_block_timestamp > target_timestamp:
                latest_block = middle_block - 1
            else:
                # Exact timestamp match found
                return middle_block

        return earliest_block

    def get_dex_events_list(self, start_block, end_block):
        logger.info("Get DEX event list")
        cursor = self._dex_events_col.find(
            filter={
                "block_number": {"$gte": start_block, "$lte": end_block},
                "event_type": "SWAP",
            },
            projection={
                "amount0": 1,
                "amount1": 1,
                "amount0_in": 1,
                "amount0_out": 1,
                "amount1_in": 1,
                "amount1_out": 1,
                "contract_address": 1,
                "wallet": 1,
                "block_timestamp": 1,
                "transaction_hash": 1,
            },
        ).sort("block_number", 1)

        docs = list(cursor)
        return docs
