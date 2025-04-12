import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_community import MongoDBCommunity
from src.services.core.news_posting import NewsPostingService
from src.utils.logger import get_logger

logger = get_logger("Post News Job")


class PostNewsJob(SchedulerJob):
    def __init__(self, interval, delay, run_now, category, token, time_interval):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.category = category
        self.token = token
        self.mongodb_community = MongoDBCommunity()
        self.news_posting_service = NewsPostingService(symbol=token, time_interval=time_interval)

    def post_news(self):
        self.news_posting_service.post_news_daily()

    def _execute(self, *args, **kwargs):
        logger.info("Post News Job started")
        begin = time.time()
        self.post_news()
        end = time.time()
        logger.info(f"Post News Job finished in {end - begin} seconds")
