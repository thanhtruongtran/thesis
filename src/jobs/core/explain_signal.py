import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.services.core.signal_explaning import SignalExplainer
from src.utils.logger import get_logger

logger = get_logger("Explain Signal Job")


class ExplainSignalJob(SchedulerJob):
    def __init__(self, interval, delay, run_now):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.signal_explainer = SignalExplainer()

    def explain_signal(self):
        self.signal_explainer.get_and_explain_signals()

    def _execute(self, *args, **kwargs):
        logger.info("Explain Signal Job started")
        begin = time.time()
        self.explain_signal()
        end = time.time()
        logger.info(f"Explain Signal Job finished in {end - begin} seconds")
