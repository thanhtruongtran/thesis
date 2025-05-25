import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.databases.mongodb_community import MongoDBCommunity
from src.services.core.ner import NERService
from src.utils.logger import get_logger

logger = get_logger("NER News Job")


class NERNewsJob(SchedulerJob):
    def __init__(self, interval, delay, run_now):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.ner_service = NERService()
        self.mongodb_community = MongoDBCommunity()

    def _execute(self, *args, **kwargs):
        logger.info("NER News Job started")
        begin = time.time()
        self.extract_entities()
        end = time.time()
        logger.info(f"NER News Job finished in {end - begin} seconds")

    def extract_entities(self):
        news_docs = self.mongodb_community.get_collection("news_articles").find(
            {"entities": {"$exists": False}}
        ).sort("publish_date_timestamp", -1).limit(10)
        news_docs = list(news_docs)
        logger.info(f"Extracting entities from {len(news_docs)} news articles")
        for doc in news_docs:
            try:
                extracted_entities = self.ner_service.extract_entities(doc["summary"])
                entities = self.ner_service.process_entities(extracted_entities)
                self.mongodb_community.get_collection("news_articles").update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"entities": entities}},
                )
            except Exception as e:
                logger.error(f"Error extracting entities from news article {doc['_id']}: {e}")
