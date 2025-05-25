import os
import sys
import time

import torch

sys.path.append(os.getcwd())

from sentence_transformers import SentenceTransformer, util

from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger

logger = get_logger("Get Interest Feed")


class GetInterestFeed:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.mongodb_community = MongoDBCommunity()
        self.mongodb_cdp = MongoDBCDP()
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def get_user_preferences(self):
        cursor = self.mongodb_community._db["user_preferences"].find(
            {"userId": self.user_id}
        )
        cursor = list(cursor)
        preferences = cursor[0]["preferences"]
        embedding = cursor[0]["embedding"]

        return preferences, embedding

    def get_news_preferences(self):
        preferences, embedding = self.get_user_preferences()

        cursor = (
            self.mongodb_community._db["news_articles"]
            .find(
                {
                    "publish_date_timestamp": {"$gte": int(time.time()) - 86400 * 3},
                    "embedding": {"$exists": True},
                    "entities": {"$exists": True},
                    "link_entities": {"$exists": True},
                },
                {
                    "title": 1,
                    "publish_date_timestamp": 1,
                    "text": 1,
                    "summary": 1,
                    "img_url": 1,
                    "url": 1,
                    "type": 1,
                    "entities": 1,
                    "embedding": 1,
                    "link_entities": 1
                },
            )
            .sort("publish_date_timestamp", -1)
        )
        
        related_news = []
        for news in cursor:
            news_embedding = torch.tensor(news.get("embedding"))
            user_embedding = torch.tensor(embedding)

            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            news_embedding = news_embedding.to(device)
            user_embedding = user_embedding.to(device)
            cosine_similarity = util.pytorch_cos_sim(user_embedding, news_embedding)
            if cosine_similarity > 0:
                related_news.append(
                    {
                        "keyWord": news.get("title", ""),
                        "lastUpdated": news.get("publish_date_timestamp", ""),
                        "content": news.get("summary", ""),
                        "type": "news",
                        "imgUrl": news.get("img_url", ""),
                        "url": news.get("url", ""),
                        "entities": news.get("entities", []),
                        "link_entities": news.get("link_entities")
                    }
                )
        return related_news

    def get_interest_feed(self):
        timestamp = int(time.time()) - 86400 * 3
        feed = (
            self.mongodb_community._db["news_articles"]
            .find(
                {
                    "publish_date_timestamp": {"$gte": timestamp},
                    "entities": {"$exists": True},
                    "link_entities": {"$exists": True},
                },
                {
                    "title": 1,
                    "publish_date_timestamp": 1,
                    "text": 1,
                    "summary": 1,
                    "img_url": 1,
                    "url": 1,
                    "type": 1,
                    "entities": 1,
                    "link_entities": 1
                },
            )
            .sort("publish_date_timestamp", -1)
        )
        result = [
            {
                "keyWord": i["title"],
                "lastUpdated": i["publish_date_timestamp"],
                "content": i["summary"],
                "type": "news",
                "imgUrl": i["img_url"],
                "url": i["url"],
                "entities": i["entities"],
                "link_entities": i["link_entities"]
            }
            for i in feed
        ]

        return result
