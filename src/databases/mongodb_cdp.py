import time
from typing import Optional

import pandas as pd
import pymongo
from pymongo import MongoClient, ReplaceOne, UpdateOne

from src.constants.config import MongoDBCDPConfig
from src.constants.mongodb import MongoDBCDPCollections
from src.constants.time import TimeConstants
from src.utils.list_dict import delete_none, flatten_dict
from src.utils.logger import get_logger
from src.utils.text import detect_language_one_text
from src.utils.time import round_timestamp

logger = get_logger("MongoDB CDP")


class MongoDBCDP:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBCDPConfig.CONNECTION_URL

        self.previous_14days = int(time.time() - TimeConstants.DAYS_7 * 2)
        self.previous_7days = int(time.time() - TimeConstants.DAYS_7)
        self.previous_3days = int(time.time() - TimeConstants.DAYS_3)
        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)
        self._db = self.connection[MongoDBCDPConfig.DATABASE]
        self._projects = self._db["projects"]

        self.db_name = MongoDBCDPConfig.DATABASE
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

    def _get_project_id(self):
        cursor = self._projects.find({}, {"projectId": 1})
        project_id_lst = []
        for cur in cursor:
            project_id_lst.append(cur["projectId"])

        return project_id_lst

    def get_tweets_by_author(self, col_name: str, kol_name: str):
        return self._db[col_name].find(
            {
                "authorName": kol_name,
                "blockchainContent": {"$exists": False},
                "timestamp": {"$gte": time.time() - TimeConstants.DAYS_2},
            },
            {"_id": 1, "text": 1},
        )

    def get_tokens(self, col_name: str):
        token_cursor = self._db[col_name].find(
            {
                "symbol": {"$exists": True},
            },
            {"symbol": 1},
        )
        return [str(tok["symbol"]).strip() for tok in token_cursor]

    def get_tweets_by_authorName(
        self, col_name: str, kol_name: str, time_interval=TimeConstants.DAYS_7
    ):
        return self._db[col_name].find(
            {
                "authorName": kol_name,
                "timestamp": {"$gte": time.time() - time_interval},
                "text": {"$not": {"$regex": r"^RT", "$options": "i"}},
                "keywords": {"$exists": True},
            },
            {"_id": 1, "text": 1, "author": 1, "keywords": 1},
        )

    def get_id_by_userName(self, col_name: str, kol_name: str):
        user = self._db[col_name].find_one({"userName": kol_name}, {"_id": 1})
        if user:
            return user["_id"]
        else:
            return None

    def get_tweets_by_filters_limit_per_kols(
        self,
        col_name,
        limit_tweet_per_KOL: int,
        kol_ids: list[str],
        minium_characters: int = 0,
    ):
        query_tweet = [
            {
                "$match": {
                    "$and": [
                        {"author": {"$in": kol_ids}},
                        {"$expr": {"$gt": [{"$strLenCP": "$text"}, minium_characters]}},
                        {"text": {"$not": {"$regex": "^RT"}}},
                    ]
                }
            },
            {"$group": {"_id": "$author", "tweets": {"$push": "$$ROOT"}}},
            {"$project": {"tweets": {"$slice": ["$tweets", limit_tweet_per_KOL]}}},
            {"$unwind": "$tweets"},
            {"$replaceRoot": {"newRoot": "$tweets"}},
        ]

        return self._db[col_name].aggregate(query_tweet)

    def get_documents_by_time(self, social, col_name: str):
        interval = TimeConstants.DAYS_7
        timestamp = round_timestamp(int(time.time()), TimeConstants.A_DAY)
        time_interval = timestamp - int(interval)
        if social == "twitter":
            return self._db[col_name].find({"timestamp": {"$gt": time_interval}})

        if social == "telegram":
            return self._db[col_name].find(
                {"timestamp": {"$gt": time_interval}, "announcement": True}
            )

    def get_latest_keyword_tweets(self):
        data = self._db["tweets"].find({"timestamp": {"$gte": self.previous_7days}})
        data_lst = []
        for da in data:
            da_dct = {}
            if "keyWords" in da:
                da_dct["_id"] = da["_id"]
                da_dct["authorName"] = da["authorName"]
                da_dct["timestamp"] = da["timestamp"]
                da_dct["keyWords"] = da["keyWords"]
                data_lst.append(da_dct)

        return data_lst

    def get_all_centic_accounts(self):
        following_cursor = self._db["twitter_followings"].find(
            {"project": {"$exists": True}, "followings": {"$exists": True}}
        )
        user_following_dct = dict()
        for cur in following_cursor:
            user_following_dct[cur["project"]] = cur["followings"]

        return user_following_dct

    def get_tweet(self):
        tweets_lst = []
        cursor = self.get_documents_by_time(social="twitter", col_name="tweets")
        for da in cursor:
            da_dct = {}
            da_dct["authorName"] = da["authorName"]
            da_dct["_id"] = da["_id"]
            da_dct["timestamp"] = da["timestamp"]
            da_dct["content"] = da["text"]
            tweets_lst.append(da_dct)
            df_twitter = pd.DataFrame(tweets_lst)

        return df_twitter

    def get_tele(self):
        teles_lst = []
        cursor = self.get_documents_by_time(
            social="telegram", col_name="telegram_messages"
        )
        for da in cursor:
            if "message" in da:
                da_dct = {}
                da_dct["content"] = da["message"]
                da_dct["timestamp"] = da["timestamp"]
                da_dct["_id"] = da["_id"]
                teles_lst.append(da_dct)
                df_tele = pd.DataFrame(teles_lst)

        return df_tele

    def get_kols(self):
        kols_lst = []
        cursor = self._db["twitter_kols_elite"].find({})
        for da in cursor:
            kols_lst.append(da)

        return kols_lst

    def get_kols_tweets(self, kol, period=TimeConstants.DAYS_7 * 2):
        kol_tweets_lst = []
        cursor = (
            self._db["tweets"]
            .find({"authorName": kol, "timestamp": {"$gte": int(time.time() - period)}})
            .sort("timestamp", -1)
        )
        for da in cursor:
            kol_tweets_lst.append(da)

        kol_tweets_dict = {}
        for tweet in kol_tweets_lst:
            if tweet["authorName"] in kol_tweets_dict:
                kol_tweets_dict[tweet["authorName"]].append(tweet["text"])
            else:
                kol_tweets_dict[tweet["authorName"]] = [tweet["text"]]

        return kol_tweets_dict

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

    def get_twitter_users(self, accounts, projection=None):
        filter_ = {"userNameLower": {"$in": [account.lower() for account in accounts]}}

        try:
            _twitter_users_col = self.get_collection(
                mongo_collection=MongoDBCDPCollections.twitter_users
            )
            cursor = _twitter_users_col.find(filter=filter_, projection=projection)
            return list(cursor)
        except Exception as e:
            logger.exception(e)

        return []

    def get_config(self, key, projection=None):
        filter_ = {"_id": key}

        try:
            _configs_col = self.get_collection(
                mongo_collection=MongoDBCDPCollections.configs
            )
            config = _configs_col.find_one(filter=filter_, projection=projection)
            return config
        except Exception as e:
            logger.exception(e)

        return None

    def get_configs(self, keys, projection=None):
        filter_ = {"_id": {"$in": keys}}

        try:
            _configs_col = self.get_collection(
                mongo_collection=MongoDBCDPCollections.configs
            )
            cursor = _configs_col.find(filter=filter_, projection=projection)
            return cursor
        except Exception as e:
            logger.exception(e)

        return []

    def get_twitter_quality_count(self, project_id):
        _configs_col = self.get_collection(
            mongo_collection=MongoDBCDPCollections.configs
        )
        result = _configs_col.find_one(
            {"_id": f"{project_id}_twitter_follower_quality"}
        )
        return result

    def get_twitter_country_follower(self, project_id):
        _configs_col = self.get_collection(
            mongo_collection=MongoDBCDPCollections.configs
        )
        result = _configs_col.find_one(
            {"_id": f"{project_id}_twitter_follower_country"}
        )
        return result

    def get_twitter_tweets_by_accounts(
        self, accounts, start_timestamp, end_timestamp, projection=None
    ):
        filter_ = {
            "authorNameLower": {"$in": [account.lower() for account in accounts]},
            "timestamp": {"$gte": start_timestamp, "$lte": end_timestamp},
        }

        try:
            _twitter_tweets_col = self.get_collection(
                mongo_collection=MongoDBCDPCollections.twitter_tweets
            )
            cursor = _twitter_tweets_col.find(
                filter=filter_, projection=projection
            ).batch_size(10000)
            return cursor.sort("timestamp", 1)
        except Exception as e:
            logger.exception(e)

        return []

    def get_top_tweets_by_token(self, token):
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": round_timestamp(time.time() - TimeConstants.DAYS_2)
                    },
                    "mentionedTokens": {"$in": [token]},
                    # "keyWords": {"$exists": True},
                },
            },
            {"$sort": {"views": -1}},
            {
                "$project": {
                    "_id": 1,
                    "author": 1,
                    "authorName": 1,
                    "timestamp": 1,
                    "text": 1,
                    "views": 1,
                    "mentionedTokens": 1,
                    "keyWords": 1,
                }
            },
        ]
        cursor = self._db["tweets"].aggregate(pipeline)
        tweets = []
        for tweet in cursor:
            if len(tweets) >= 5:
                break
            if (
                (len(tweet["text"]) > 50)
                & ("RT @" not in tweet["text"])
                & (len(tweet["mentionedTokens"]) <= 3)
                & (detect_language_one_text(tweet["text"]) == "en")
            ):
                tweets.append(tweet["text"])

        return tweets

    def get_top_tweets_by_keyword(self, keyword, agentTags=False):
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$gte": int(time.time() - TimeConstants.DAYS_2)},
                    "text": {"$regex": keyword},
                    "isUsedByAgents": {"$exists": False},
                },
            },
            {"$sort": {"views": -1}},
            {
                "$project": {
                    "_id": 1,
                    "author": 1,
                    "authorName": 1,
                    "timestamp": 1,
                    "text": 1,
                    "views": 1,
                    "mentionedTokens": 1,
                    "keyWords": 1,
                }
            },
        ]
        cursor = self._db["tweets"].aggregate(pipeline)
        tweets = []
        tagged_tweets = []

        for tweet in cursor:
            if len(tweets) >= 5:
                break
            if (
                (len(tweet["text"]) > 50)
                & ("RT @" not in tweet["text"])
                & (detect_language_one_text(tweet["text"]) == "en")
            ):
                tweets.append(tweet["text"])

                if agentTags is True:
                    tweet["isUsedByAgents"] = True
                    tagged_tweets.append(tweet)

        if len(tagged_tweets) > 0:
            self.update_docs(collection_name="tweets", data=tagged_tweets, merge=False)

        return tweets

    def update_and_replace_data(self, collection_name, data):
        bulk_updates = [
            ReplaceOne({"_id": doc["_id"]}, doc, upsert=True) for doc in data
        ]
        self._db[f"{collection_name}"].bulk_write(bulk_updates)
