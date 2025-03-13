import time

import pymongo
from pymongo import MongoClient, UpdateOne

from src.constants.config import MongoDBExtraConfig
from src.constants.time import TimeConstants
from src.utils.list_dict import delete_none, flatten_dict
from src.utils.logger import get_logger

logger = get_logger("MongoDB Extra")


class MongoDBExtra:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBExtraConfig.CONNECTION_URL

        self.previous_14days = int(time.time() - TimeConstants.DAYS_7 * 2)
        self.previous_7days = int(time.time() - TimeConstants.DAYS_7)
        self.previous_3days = int(time.time() - TimeConstants.DAYS_3)
        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)
        self._db = self.connection[MongoDBExtraConfig.DATABASE]

    @staticmethod
    def create_update_doc(document, keep_none=False, merge=True, shard_key=None):
        unset, set_, add_to_set = [], [], []
        if not keep_none:
            doc = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    tmp = {"_id": document["_id"], key: ""}
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    unset.append(tmp)
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    tmp = {
                        "_id": document["_id"],
                        key: {"$each": [i for i in value if i]},
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    add_to_set.append(tmp)
                else:
                    tmp = {"_id": document["_id"], key: value}
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    set_.append(tmp)

        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))

        return unset, set_, add_to_set

    def update_docs(
        self,
        collection_name,
        data,
        keep_none=False,
        merge=True,
        shard_key=None,
        flatten=True,
    ):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        try:
            col = self._db[collection_name]
            # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
            bulk_operations = []
            if not flatten:
                if not shard_key:
                    bulk_operations = [
                        UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True)
                        for item in data
                    ]
                else:
                    bulk_operations = [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {"$set": item},
                            upsert=True,
                        )
                        for item in data
                    ]
                col.bulk_write(bulk_operations)
                return

            for document in data:
                unset, set_, add_to_set = self.create_update_doc(
                    document, keep_none, merge, shard_key
                )
                if not shard_key:
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$unset": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in unset
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$set": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in set_
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"]},
                            {
                                "$addToSet": {
                                    key: value
                                    for key, value in item.items()
                                    if key != "_id"
                                }
                            },
                            upsert=True,
                        )
                        for item in add_to_set
                    ]
                if shard_key:
                    keys = ["_id", shard_key]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$unset": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in unset
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$set": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in set_
                    ]
                    bulk_operations += [
                        UpdateOne(
                            {"_id": item["_id"], shard_key: item[shard_key]},
                            {
                                "$addToSet": {
                                    key: value
                                    for key, value in item.items()
                                    if key not in keys
                                }
                            },
                            upsert=True,
                        )
                        for item in add_to_set
                    ]
            col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def get_largest_flag(self, db):
        largest_flag = -1

        query = {"flagged": {"$exists": "True"}}
        projection = {"flagged": 1}
        cursor = (
            self._db[f"{db}"]
            .find(query, projection=projection)
            .sort("flagged", pymongo.DESCENDING)
            .limit(1)
        )
        for cur in cursor:
            largest_flag = cur["flagged"]

        return largest_flag
