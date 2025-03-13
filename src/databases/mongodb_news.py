import pandas as pd
from pymongo import MongoClient

from src.constants.config import MongoDBNewsConfig
from src.constants.time import TimeConstants
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("MongoDB News")


class MongoDBNews:
    def __init__(self, connection_url=None):
        if not connection_url:
            connection_url = MongoDBNewsConfig.CONNECTION_URL

        self.connection_url = connection_url.split("@")[-1]
        self.connection = MongoClient(connection_url)
        self._db = self.connection[MongoDBNewsConfig.DATABASE]

    def get_keywords_data(self, start_time=None, end_time=None, interval=None):
        start_time = round_timestamp(start_time, TimeConstants.A_DAY)
        end_time = round_timestamp(end_time, TimeConstants.A_DAY)
        start_time = pd.to_datetime(start_time, unit="s").strftime(
            "%Y-%m-%dT%H:%M:%S.000+00:00"
        )
        end_time = pd.to_datetime(end_time, unit="s").strftime(
            "%Y-%m-%dT%H:%M:%S.000+00:00"
        )
        start_time = pd.to_datetime(start_time)
        end_time = pd.to_datetime(end_time)

        collection = "keyword_data"

        cursor = self._db[collection].find(
            {
                "timestamp": {"$gte": start_time, "$lt": end_time},
                "time_interval": interval,
            },
            {
                "timestamp": 1,
                "time_interval": 1,
                "data.keyword": 1,
                "data.description": 1,
                "data.summary": 1,
                "data.tokens_affected": 1,
            },
        )

        result = []
        for da in cursor:
            result.append(
                {
                    "timestamp": pd.to_datetime(da.get("timestamp")).strftime(
                        "%Y-%m-%dT%H:%M:%S.000+00:00"
                    ),
                    "interval": da.get("time_interval"),
                    "data": da.get("data"),
                }
            )

        return result
