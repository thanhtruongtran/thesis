import time

from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.postgresql import PostgresDB
from src.jobs.cli import CLIJob
from src.services.load_data.agents_token_transfering import AgentsTokenTransferService
from src.utils.logger import get_logger


class AgentsTokenTransferJob(CLIJob):
    def __init__(
        self,
        chains,
        batch_size,
        interval,
    ):
        super().__init__(interval=interval)
        self.interval = interval
        self.logg = get_logger("Transfer ")
        self.chains = chains
        self.mongodb_cdp = MongoDBCDP()
        self.batch_size = batch_size

    def _pre_start(self):
        self.logg.info("Start to query agent tokens ...")

    def _execute(self):
        for chain in self.chains:
            pg_rag = PostgresDB(chain_id=chain, rag=True)
            pg_rag.create_table()

            pg = PostgresDB(chain_id=chain)
            print("Sleeping 10s ...")
            time.sleep(10)
            job = AgentsTokenTransferService(
                chain=chain,
                batch_size=self.batch_size,
                PostgreDB=pg,
                PostgreRagDB=pg_rag,
            )
            job.run()
