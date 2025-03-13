import time
from typing import List

import pymongo
from pymongo import MongoClient

from src.constants.config import MongoDBETLConfig
from src.constants.mongodb import MongoDBCollections
from src.constants.time import TimeConstants
from src.utils.logger import get_logger

logger = get_logger("MongoDB ETL")


class MongoDBETL:
    def __init__(self, chain, connection_url=None):
        if not connection_url:
            connection_url = MongoDBETLConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)
        if chain == "bsc":
            self._db = self.connection[f"{MongoDBETLConfig.DATABASE}"]
        else:
            self._db = self.connection[f"{chain}_{MongoDBETLConfig.DATABASE}"]

        self.block_collection = self._db[MongoDBCollections.blocks]

    def get_block_number_by_timestamp(self, timestamp):
        block = self._db["transactions"].find(
            {"block_timestamp": {"$gte": timestamp}}, {"block_number": 1}
        )
        return block[0]["block_number"]

    def get_previous_3month_block(self):
        time_now = int(int(time.time()) - TimeConstants.A_DAY)
        lower_time = int(int(time_now) - TimeConstants.MONTHS_3)
        query = {"timestamp": {"$gte": lower_time}}
        projection = {"timestamp": 1, "number": 1}
        cursor = (
            self._db.blocks.find(filter=query, projection=projection)
            .sort("timestamp", pymongo.ASCENDING)
            .limit(1)
        )
        return {block["number"]: block["timestamp"] for block in cursor}

    def get_largest_block(self):
        uppper_time = int(int(time.time()) - TimeConstants.A_DAY)
        # uppper_time = 1714037151
        query = {"timestamp": {"$lte": uppper_time}}
        projection = {"timestamp": 1, "number": 1}
        cursor = (
            self._db.blocks.find(query, projection=projection)
            .sort("timestamp", pymongo.DESCENDING)
            .limit(1)
        )
        return {block["number"]: block["timestamp"] for block in cursor}

    def get_block_number_to_timestamp(self, start_block, end_block):
        cursor = self.get_blocks_in_range(
            start_block=start_block,
            end_block=end_block,
            projection=["number", "timestamp"],
        )
        return {block["number"]: block["timestamp"] for block in cursor}

    def get_blocks_in_range(self, start_block, end_block, projection: List = None):
        filter_ = {"number": {"$gte": start_block, "$lte": end_block}}
        if not projection:
            cursor = self.block_collection.find(filter_).batch_size(1000)
        else:
            cursor = self.block_collection.find(
                filter=filter_, projection=projection
            ).batch_size(1000)
        return cursor
