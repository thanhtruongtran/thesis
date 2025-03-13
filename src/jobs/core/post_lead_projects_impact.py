import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.services.core.lead_projects_impact import LeadProjectsImpact
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("Post Lead Projects Impact Job")


class PostLeadProjectsImpactJob(SchedulerJob):
    def __init__(self, interval, delay, run_now, category):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.category = category
        self.mongodb_cdp = MongoDBCDP()
        self.lead_projects_impact = LeadProjectsImpact()

    def post_lead_projects_impact(self):
        results = self.lead_projects_impact.lead_project_impact_posting()

        posts = []
        timestamp = round_timestamp(time.time())
        for k, w in results.items():
            posts.append(
                {
                    "_id": f"{k}_{timestamp}",
                    "lastUpdated": timestamp,
                    "upToDate": True,
                    "content": w,
                    "type": "projects_impact",
                }
            )
        try:
            self.mongodb_cdp.update_docs("agent_contents", posts)
        except Exception as e:
            logger.error(f"Error when posting lead projects impact: {e}")


    def _execute(self, *args, **kwargs):
        logger.info("Post Lead Projects Impact Job started")
        begin = time.time()
        if self.category == "projects_impact":
            self.post_lead_projects_impact()
        end = time.time()
        logger.info(f"Post Lead Projects Impact Job finished in {end - begin} seconds")
