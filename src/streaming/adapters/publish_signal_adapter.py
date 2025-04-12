import time

from src.constants.time import TimeConstants
from src.databases.blockchain_etl import BlockchainETL
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_dex import MongoDBDex
from src.databases.mongodb_sc_label import MongoDBSCLabel
from src.jobs.signals.publish_signal_job import PublishSignalJob
from src.streaming.exporters.signal_exporter import SignalExporter
from src.utils.file import write_last_time_running_logs
from src.utils.logger import get_logger

logger = get_logger('Publish Signal Adapter')


class PublishSignalAdapter:
    def __init__(self, importer: BlockchainETL, exporter: SignalExporter, dex_db: MongoDBDex, klg_db: MongoDBKLG, sc_label_db: MongoDBSCLabel, collector_id="streaming_collector", batch_size=4, max_workers=8, query_batch_size=2000, forks=None, monitor=True, chain_id='0x138de'):   
        self.chain_id = chain_id
        self.collector_id = collector_id

        self.batch_size = batch_size
        self.max_workers = max_workers

        self.query_batch_size = query_batch_size

        self._exporter = exporter
        self._importer = importer

        self.dex_db = dex_db
        self.klg_db = klg_db
        self.sc_label_db = sc_label_db

        self.forks = forks
        self.monitor = monitor

    def switch_provider(self):
        # Switch provider
        pass

    def get_current_block_number(self):
        current_block = None
        for collector_id in self.collector_id:
            block = self._importer.get_last_block_number(collector_id=collector_id)
            if current_block is None or block < current_block:
                current_block = block
        return current_block

    def enrich_all(self, start_block=0, end_block=0):
        start = time.time()
        logger.info(f"Start enrich block {start_block} - {end_block} ")
        self.enrich_data(start_block, end_block)
        end = time.time()
        logger.info(f"Enrich block {start_block} - {end_block} take {end - start}")

    def enrich_data(self, start_block, end_block):
        job = PublishSignalJob(
            start_block=start_block,
            end_block=end_block,
            batch_size=self.batch_size,
            max_workers=self.max_workers,
            importer=self._importer,
            exporter=self._exporter,
            chain_id=self.chain_id,
            query_batch_size=self.query_batch_size,
            dex_db=self.dex_db,
            klg_db=self.klg_db,
            sc_label_db=self.sc_label_db,
            forks=self.forks,
            collector_id=self.collector_id,
        )
        job.run()

        if self.monitor:
            write_last_time_running_logs(
                stream_name=f'{self.__class__.__name__}_{self.chain_id}',
                timestamp=int(time.time()),
                threshold=TimeConstants.MINUTES_15
            )
