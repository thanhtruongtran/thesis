import os
import sys
import time

sys.path.append(os.getcwd())

from src.constants.llm.agent_prompt import NewsPromptTemplate
from src.services.llm.communication import LLMCommunication
from src.databases.mongodb_community import MongoDBCommunity
from src.databases.mongodb_cdp import MongoDBCDP
from src.utils.logger import get_logger

logger = get_logger("Get Interest Feed")


class GetInterestFeed:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.mongodb_community = MongoDBCommunity()
        self.mongodb_cdp = MongoDBCDP()
        self.llm = LLMCommunication()

    def get_interest(self):
        cursor = self.mongodb_community._db["chat_query"].find(
            {
                "_id": self.user_id
            }
        )
        interest = cursor[0]["interest"]
        interest = list(interest.values())
        interest = [i.lower() for i in interest]
        return interest
    
    def search_news_related(self, interest):
        # search by keywords
        timestamp = int(time.time()) - 86400 * 1
        cursor = self.mongodb_community._db["news_articles"].find(
            {
                "keywords": {"$in": interest},
                "publish_date_timestamp": {"$gte": timestamp}
            }
        )

        news = [
            {
                "title": i["title"],
                "summary": i["summary"],
                "keywords": i["keywords"],
                "publish_date": i["publish_date"],
                "publish_date_timestamp": i["publish_date_timestamp"]
            }
            for i in cursor
        ]

        # keep news that its title contains at least 1 keyword
        news = [
            i for i in news if any(j in i["title"].lower() for j in interest)
        ]

        return news
    
    def search_news_related_in_twitter(self, interest):
        # search by keywords
        timestamp = int(time.time()) - 86400 * 1
        list_news = []
        token = interest[2]


        cursor = self.mongodb_cdp._db["tweets"].find(
            {
                "text": {"$regex": f"${token}"},
                "timestamp": {"$gte": timestamp}
            }
        )
        list_news += list(cursor)

        # keep news that have maximum views, likes
        news = []
        for i in list_news:
            if i["views"] > 1000 or i["likes"] > 1000:
                news.append(i)

        # sort by views, likes; remove news that have same text
        news = sorted(news, key=lambda x: x["views"] + x["likes"], reverse=True)
        processed_news = []
        for i in news:
            if i["text"] not in [j["text"] for j in processed_news]:
                processed_news.append(i)
        news = processed_news[:5]

        news = [
            {
                "title": "",
                "summary": i["text"],
                "keywords": "",
                "publish_date": i["created"],
                "publish_date_timestamp": i["timestamp"]
            }
            for i in news
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
    
    def save_interest_feed(self):
        interest = self.get_interest()
        news = self.search_news_related(interest)
        # news = news[:2]
        # if len(news) < 3:
        #     news += self.search_news_related_in_twitter(interest)

        list_news = []
        for i in news:
            response = self.post_news(i)
            list_news.append({
                "_id": f"{self.user_id}_{i['publish_date_timestamp']}",
                "userId": self.user_id,
                "lastUpdated": i["publish_date_timestamp"],
                "keyWord": i["title"],
                "content": response,
                "type": "news"
            })

        self.mongodb_community.update_docs(
            collection_name="news_contents",
            data=list_news
        )
        logger.info("Save interest feed successfully.")
        return {"status": "success"}
    
    def get_interest_feed(self):
        timestamp = int(time.time()) - 86400 * 3
        feed = self.mongodb_community._db["news_articles"].find(
            {
                "publish_date_timestamp": {"$gte": timestamp},
            }
        )
        result = [
            {
                "keyWord": i["title"],
                "lastUpdated": i["publish_date_timestamp"],
                "content": i["summary"],
                "type": "news",
            }
            for i in list(feed)[:10]
        ]

        return result
