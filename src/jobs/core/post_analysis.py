import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_community import MongoDBCommunity
from src.services.core.analysis_posting import AnalysisPostingService
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger

logger = get_logger("PostAnalysisJob")


class PostAnalysisJob(SchedulerJob):
    def __init__(
        self,
        interval,
        delay,
        run_now,
    ):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.llm = LLMCommunication()
        self.cdp = MongoDBCDP()
        self.community = MongoDBCommunity()
        self.interval = interval

    def _execute(self, *args, **kwargs):
        begin_time = time.time()
        logger.info("Start Post Analysis Job")
        self.post_analysis()
        logger.info(f"End Post Analysis Job in {time.time() - begin_time} seconds")

    def post_analysis(self):
        analysis_service = AnalysisPostingService()
        data = analysis_service.post_analysis()

        self.community.update_docs(
            collection_name="analytics",
            data=data,
        )
