from typing import Optional

from pymongo import MongoClient, UpdateOne

from src.constants.config import MongoDBCommunityConfig
from src.utils.list_dict import delete_none, flatten_dict, update_dict_of_dict
from src.utils.logger import get_logger

logger = get_logger("MongoDB Community")


class MongoDBCommunity:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBCommunityConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)
        self._db = self.connection[MongoDBCommunityConfig.DATABASE]

        self.db_name = MongoDBCommunityConfig.DATABASE
        self.client: Optional[MongoClient] = None
        self.connection_url_not_split = connection_url

    def get_conn(self) -> MongoClient:
        """Fetches PyMongo Client."""
        if self.client is not None:
            return self.client

        self.client = MongoClient(self.connection_url_not_split)
        return self.client

    def get_collection(self, mongo_collection: str, mongo_db: str = None):
        """
        Fetches a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db or self.db_name
        mongo_conn: MongoClient = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

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

    def get_tokens(self):
        list_tokens = []
        cursor1 = self._db["cookiefun_agents"].find({})
        for da in cursor1:
            list_tokens.append(da.get("ticker"))

        cursor2 = self._db["agentfi_agents"].find({})
        for da in cursor2:
            list_tokens.append(da.get("symbol"))

        return list_tokens

    def query_historical_prices(self, chains):
        logger.info("Getting token historical prices")
        token_price = self._db["top_token_multichain"].find(
            {"chainId": {"$in": chains}}
        )
        historical_token_price = dict()

        for chain_id in chains:
            historical_token_price[f"{chain_id}"] = {}

        for token in token_price:
            token_info = dict()
            token_dct = dict()
            address = token["address"]
            price_history = token["priceHistory"]
            chain_id = token["chainId"]
            token_dct[f"{address}"] = price_history
            token_info[f"{chain_id}"] = token_dct

            historical_token_price = update_dict_of_dict(
                historical_token_price, token_info
            )

        return historical_token_price

    def get_all_chat_ids(self):
        chat_ids = []
        cursor = self._db["telegram_users"].find({})
        for da in cursor:
            chat_ids.append(da.get("chat_id"))
        return chat_ids

    def save_news_view_history(self, user_id: str, news_id: str, timestamp: int, entities):
        try:
            collection = self._db["news_view_history"]
            doc = {"userId": user_id, "newsId": news_id, "timestamp": timestamp, "entities": entities}
            collection.insert_one(doc)
            logger.info(f"Saved news view history: {doc}")
            return True
        except Exception as e:
            logger.error(f"Error saving news view history: {e}")
            return False
