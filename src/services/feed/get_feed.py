import os
import sys
import time

sys.path.append(os.getcwd())

from dotenv import load_dotenv

from src.databases.mongodb_cdp import MongoDBCDP
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("Get Feed")


class GetFeed:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.mongodb = MongoDBCDP()

    def get_feed(self):
        timestamp = int(time.time()) - 86400 * 2
        feed = self.mongodb._db["agent_contents"].find(
            {
                "type": "analysis",
                "lastUpdated": {"$gte": timestamp},
            }
        )
        feed = [
            {
                "keyWord": i["keyWord"],
                "lastUpdated": i["lastUpdated"],
                "content": i["content"],
            }
            for i in feed
        ]

        return feed
