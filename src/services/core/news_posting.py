import time

from src.constants.llm.agent_prompt import NewsPromptTemplate
from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_community import MongoDBCommunity
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger

logger = get_logger("News Posting Service")


class NewsPostingService:
    def __init__(self, symbol=None, time_interval=None):
        self.mongodb_community = MongoDBCommunity()
        self.mongodb_cdp = MongoDBCDP()
        self.mongodb_klg = MongoDBKLG()
        self.llm = LLMCommunication()
        self.symbol = symbol
        self.time_interval = time_interval

    def get_data(self):
        timestamp = int(time.time()) - TimeConstants.A_DAY * self.time_interval
        cursor = self.mongodb_community._db["news_articles"].find(
            {
                "publish_date_timestamp": {"$gte": timestamp},
                "$or": [
                    {"updated": False},
                    {"updated": {"$exists": False}}
                ]
            }
        )

        news = [
            {
                "_id": i["_id"],
                "title": i["title"],
                "summary": i["summary"],
                "keywords": i["keywords"],
                "publish_date": i["publish_date"],
                "publish_date_timestamp": i["publish_date_timestamp"]
            }
            for i in cursor
        ]

        return news

    def post_news(self, info):
        news_prompt_template = NewsPromptTemplate()
        template = news_prompt_template.create_template()

        prompt = template.format(
            information_keyword=info.get("keywords"),
            information_description=info.get("title"),
            information_summary=info.get("summary")
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_news_daily(self):
        news = self.get_data()
        list_news = []
        for i in news:
            response = self.post_news(i)
            list_news.append({
                "_id": i["_id"],
                "lastUpdated": i["publish_date_timestamp"],
                "title": i["title"],
                "keywords": i["keywords"],
                "content": response,
            })

        self.mongodb_community.update_docs(
            collection_name="news_contents",
            data=list_news
        )

        list_news_ids = [i["_id"] for i in list_news]
        self.mongodb_community._db["news_articles"].update_many(
            {"_id": {"$in": list_news_ids}},
            {"$set": {"updated": True}}
        )
        