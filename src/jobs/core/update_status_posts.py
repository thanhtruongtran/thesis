import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("Update Status Posts Job")


class UpdateStatusPostsJob(SchedulerJob):
    def __init__(self, interval, delay, run_now, collection):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.mongodb_cdp = MongoDBCDP()
        self.collection = collection

    def execute(self):
        timestamp = round_timestamp(time.time())

        cursor = self.mongodb_cdp._db[self.collection].find(
            {"lastUpdated": {"$lt": timestamp}, "upToDate": True}
        )
        data = []
        for doc in cursor:
            data.append({"_id": doc["_id"], "upToDate": False})

        if len(data) == 0:
            logger.info("No status posts to update")
            return
        else:
            logger.info(f"Updating {len(data)} status posts")
            self.mongodb_cdp.update_docs(self.collection, data, merge=True)

    def _execute(self, *args, **kwargs):
        logger.info("Update Status Posts Job started")
        begin = time.time()
        self.execute()
        end = time.time()
        logger.info(f"Update Status Posts Job finished in {end - begin} seconds")
