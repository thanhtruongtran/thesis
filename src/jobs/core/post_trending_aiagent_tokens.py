import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.services.core.trending_aiagent_tokens_posting import (
    TrendingAIAgentTokensPostingService,
)
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("Trending AI Agent Tokens Posting Job")


class TrendingAIAgentTokensPostingJob(SchedulerJob):
    def __init__(self, interval, delay, run_now):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.mongodb_cdp = MongoDBCDP()
        self.trending_aiagent_tokens_posting_service = (
            TrendingAIAgentTokensPostingService()
        )

    def execute(self):
        results = (
            self.trending_aiagent_tokens_posting_service.post_trending_aiagent_tokens()
        )

        news = []
        timestamp = round_timestamp(time.time())
        for token, text in results.items():
            symbol = token.split(" | ")[0]
            chain = token.split(" | ")[1]
            contract_address = token.split(" | ")[2]
            news.append(
                {
                    "_id": f"{symbol}_{chain}_{contract_address}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "content": text,
                    "keyWord": symbol,
                    "type": "news_aiagent_tokens",
                }
            )
        self.mongodb_cdp.update_docs("agent_contents", news)

    def _execute(self, *args, **kwargs):
        logger.info("Trending AI Agent Tokens Posting Job started")
        begin = time.time()
        self.execute()
        end = time.time()
        logger.info(
            f"Trending AI Agent Tokens Posting Job finished in {end - begin} seconds"
        )
