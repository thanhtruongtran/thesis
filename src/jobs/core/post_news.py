import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.services.core.news_posting import NewsPostingService
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("Post News Job")


class PostNewsJob(SchedulerJob):
    def __init__(self, interval, delay, run_now, category, token):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.category = category
        self.token = token
        self.mongodb_cdp = MongoDBCDP()
        self.news_posting_service = NewsPostingService(symbol=token)

    def post_news(self):
        results_news, results_reply_news = self.news_posting_service.post_news_daily()

        news = []
        # timestamp = round_timestamp(time.time(), round_time=43200)
        # We temporary consider to update once a day
        timestamp = round_timestamp(time.time())
        for kw, text in results_news.items():
            news.append(
                {
                    "_id": f"{kw}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "content": text,
                    "keyWord": kw,
                    "type": "news",
                }
            )
        self.mongodb_cdp.update_docs("agent_contents", news)

        reply_news = []
        for kw, texts in results_reply_news.items():
            reply_news.append(
                {
                    "_id": f"{kw}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "repliesContent": texts,
                    "type": "news",
                }
            )
        self.mongodb_cdp.update_docs("agent_contents", reply_news)

    def post_lending_news(self):
        results = self.news_posting_service.post_lending_news_daily()

        news = []
        timestamp = round_timestamp(time.time())
        for kw, text in results.items():
            news.append(
                {
                    "_id": f"{kw}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "content": text,
                    "keyWord": kw,
                    "type": "lending_news",
                }
            )
        self.mongodb_cdp.update_docs("agent_lending/borrowing_contents", news)

    def post_token_news(self):
        result = self.news_posting_service.post_token_news_daily()

        timestamp = round_timestamp(time.time())
        news = [
            {
                "_id": f"{result['symbol']}_{timestamp}",
                "lastUpdated": timestamp,
                "upToDate": True,
                "content": result["content"],
                "keyWord": result["symbol"],
                "type": "token_news",
            }
        ]
        self.mongodb_cdp.update_docs("agent_contents", news)

    def post_berachain_news(self):
        results = self.news_posting_service.post_berachain_news_daily()

        news = []
        timestamp = round_timestamp(time.time())
        for kw, text in results.items():
            news.append(
                {
                    "_id": f"{kw}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "content": text,
                    "type": "berachain_news",
                }
            )
        self.mongodb_cdp.update_docs("agent_contents", news)

    def _execute(self, *args, **kwargs):
        logger.info("Post News Job started")
        begin = time.time()
        if self.token != "":
            self.post_token_news()
        else:
            if self.category == "lending":
                self.post_lending_news()
            elif self.category == "berachain":
                self.post_berachain_news()
            else:
                self.post_news()
        end = time.time()
        logger.info(f"Post News Job finished in {end - begin} seconds")
