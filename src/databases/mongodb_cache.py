import datetime
import sys
import time

from pymongo import MongoClient

from src.constants.config import MongoDBCacheConfig
from src.constants.mongodb import MongoDBCacheCollections
from src.utils.logger import get_logger

logger = get_logger("MongoDB Centic Cache")


class MongoDBCache:
    def __init__(
        self,
        connection_url=MongoDBCacheConfig.CONNECTION_URL,
        database=MongoDBCacheConfig.DATABASE,
    ):
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db_cache = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to MongoDB: {connection_url}: {e}")
            sys.exit(1)

        self._pairs_col = self.mongo_db_cache[MongoDBCacheCollections.pairs]
        self._token_prices_col = self.mongo_db_cache[
            MongoDBCacheCollections.token_prices
        ]

    def get_pairs_info(self, chain_id, contract_addr):
        doc = self._pairs_col.find_one({"_id": f"{chain_id}_{contract_addr}"})
        if (
            doc is None
            or "liquidityValueInUSD" not in doc
            or doc["liquidityValueInUSD"] < 50000
        ):
            return None, None, None
        if "tokens" not in doc or len(doc["tokens"]) < 2:
            return None, None, None
        return doc["tokens"][0], doc["tokens"][1], doc["address"]

    def get_current_price(self, chain_id, token_addr):
        start_datetime = datetime.datetime.fromtimestamp(
            int(time.time()) - 7200, tz=datetime.timezone.utc
        )
        query = {
            "metadata.tokenAddress": token_addr,
            "metadata.chainId": chain_id,
            "timestamp": {
                "$gte": start_datetime,
            },
            "price": {"$ne": None},
        }
        cursor = self._token_prices_col.find(query).sort("timestamp", -1).limit(1)
        try:
            price = cursor[0]["price"]
            logger.info(f"Price {token_addr} - {price}")
            return price
        except Exception as e:
            logger.info(f"Get current price failed: {token_addr} - {e}")
            return None

    def get_price_by_timestamp(self, timestamp, chain_id, token_addr):
        start_datetime = datetime.datetime.fromtimestamp(
            timestamp - 86400, tz=datetime.timezone.utc
        )
        to_datetime = datetime.datetime.fromtimestamp(
            timestamp, tz=datetime.timezone.utc
        )
        query = {
            "metadata.tokenAddress": token_addr,
            "metadata.chainId": chain_id,
            "timestamp": {"$gte": start_datetime, "$lte": to_datetime},
            "price": {"$ne": None},
        }
        cursor = self._token_prices_col.find(query).sort({"timestamp": -1}).limit(1)
        try:
            price = cursor[0]["price"]
            logger.info(f"Price {token_addr} - {timestamp} - {price}")
            return price
        except Exception as e:
            logger.info(f"Price {token_addr} - {timestamp} - {e}")
            return None

    def get_lowest_price_by_timestamp(self, timestamp, chain_id, token_addr):
        start_datetime = datetime.datetime.fromtimestamp(
            timestamp - 86400 * 30, tz=datetime.timezone.utc
        )
        to_datetime = datetime.datetime.fromtimestamp(
            timestamp, tz=datetime.timezone.utc
        )
        query = {
            "metadata.tokenAddress": token_addr,
            "metadata.chainId": chain_id,
            "timestamp": {"$gte": start_datetime, "$lte": to_datetime},
            "price": {"$ne": None},
        }
        cursor = self._token_prices_col.find(query).sort({"price": 1}).limit(1)
        try:
            price = cursor[0]["price"]
            logger.info(f"Price {token_addr} - {timestamp} - {price}")
            return price
        except Exception as e:
            logger.info(f"Price {token_addr} - {timestamp} - {e}")
            return None

    def get_price_list_1_month(self, timestamp, chainId, token_address):
        start_datetime = datetime.datetime.fromtimestamp(
            timestamp - 86400 * 30, tz=datetime.timezone.utc
        )
        to_datetime = datetime.datetime.fromtimestamp(
            timestamp, tz=datetime.timezone.utc
        )
        query = {
            "metadata.tokenAddress": token_address,
            "metadata.chainId": chainId,
            "timestamp": {"$gte": start_datetime, "$lte": to_datetime},
            "price": {"$ne": None},
        }
        cur = self._token_prices_col.find(query).sort({"timestamp": 1})
        price_logs = []
        for document in cur:
            price_logs.append(document["price"])
        return price_logs
