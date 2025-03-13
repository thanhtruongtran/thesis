from cli_scheduler.scheduler_job import SchedulerJob

from src.services.core.project_anomal_change_ranking import (
    ProjectChangeRankingService,
)


class ProjectChangeRankingJob(SchedulerJob):
    def __init__(
        self,
        run_now,
        delay,
        updated_collection="entity_change_ranking",
        max_workers=4,
        batch_size=100,
        interval=None,
    ):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.updated_collection = updated_collection
        self.max_workers = max_workers
        self.batch_size = batch_size

    def _execute(self, *args, **kwargs):
        job = ProjectChangeRankingService(
            updated_collection=self.updated_collection,
            max_workers=self.max_workers,
            batch_size=self.batch_size,
        )
        job.run()
