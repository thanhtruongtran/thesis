import os
import sys
import time

sys.path.append(os.getcwd())

from dotenv import load_dotenv

from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

load_dotenv()
logger = get_logger("Get Analytics")


class GetAnalytics:
    def __init__(self):
        self.mongodb_community = MongoDBCommunity()

    def get_analytics(self):
        cursor = self.mongodb_community._db["analytics"].find(
            {"lastUpdated": {"$gte": round_timestamp(time.time() - 86400)}}
        )
        return list(cursor)
