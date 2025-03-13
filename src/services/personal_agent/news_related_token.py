import os
import sys

import requests

sys.path.append(os.getcwd())

from datetime import datetime

from dotenv import load_dotenv

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_news import MongoDBNews
from src.utils.logger import get_logger

logger = get_logger("News Related Token")
load_dotenv()
### Service for getting news for a specific token ###


class NewsRelatedTokenService:
    def __init__(self):
        self.mongodb_news = MongoDBNews()
        self.mongodb_klg = MongoDBKLG()
        self.mongodb_cdp = MongoDBCDP()
        self.api_news = os.getenv("API_NEWS")

    def get_news(self, keyword, start_time, end_time, limit):
        # get news from API
        url = f"{self.api_news}/{keyword}/search_keyword_with_timestamp?start_time={start_time}&end_time={end_time}&limit={limit}"
        response = requests.get(url)
        news = response.json()
        return news

    def get_news_related_token(self, token_address):
        # get symbol of token
        cursor = self.mongodb_klg._db["smart_contracts"].find(
            {"address": token_address}
        )
        tokens = []
        for token in cursor:
            tokens.append(
                {
                    "idCoinGecko": token.get("idCoinGecko"),
                    "chainId": token.get("chainId"),
                    "address": token.get("address"),
                    "name": token.get("name"),
                    "symbol": token.get("symbol"),
                }
            )

        # get news related to token
        news = []
        start_time = datetime.strftime(datetime.now() - TimeConstants.A_DAY, "%Y-%m-%d")
        end_time = datetime.strftime(datetime.now(), "%Y-%m-%d")

        for token in tokens:
            keyword = token.get("symbol")
            limit = 2
            news.extend(self.get_news(keyword, start_time, end_time, limit))


if __name__ == "__main__":
    data = NewsRelatedTokenService().get_news(
        "PEPE", start_time="2025-03-10", end_time="2025-03-11", limit=10
    )
    print(data)
