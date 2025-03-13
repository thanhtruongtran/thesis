import os
import sys
sys.path.append(os.getcwd())
import pandas as pd

from src.databases.postgresql import PostgresDB
from src.databases.mongodb_community import MongoDBCommunity
from src.utils.blocks import mapping_timestamp_blocknumber
from src.utils.list_dict import chunk_list
from src.utils.logger import get_logger


logger = get_logger("Anomal Transaction Detection")


class AnomalTransactionDetectionService:
    def __init__(
        self,
        chain,
        batch_size=1200,
    ):
        self.chain = chain
        self.pg = PostgresDB(chain_id=chain)
        self.token_prices = MongoDBCommunity().query_historical_prices(
            chains=[self.chain]
        )

        self.multichain_transaction = mapping_timestamp_blocknumber(
            chain_ids=[self.chain]
        )

        block_list = list(
            self.multichain_transaction[chain]["block_timestamp_mappings"].keys()
        )
        self.sub_block_list = chunk_list(input_list=block_list, chunk_size=batch_size)

    def get_transactions_by_token(self, token_addresses):
        num_ = len(self.sub_block_list)
        logger.info(f"loading transfer jobs: {num_} jobs")
        loaded_block = []
        all_data = []

        for block_list in self.sub_block_list:
            try:
                data = self.pg.query_transfer_events(
                    contract_addresses=token_addresses,
                    from_block=block_list[0],
                    to_block=block_list[-1],
                    block_timestamp_mappings=self.multichain_transaction[self.chain][
                        "block_timestamp_mappings"
                    ],
                    price_history=self.token_prices[self.chain],
                )

                if len(data) > 0:
                    all_data.extend(data)

                loaded_block.append(block_list)
                num_block = len(loaded_block)
                if num_block % 100 == 0:
                    logger.info(f"Loaded {num_block} blocks")

            except Exception:
                continue

        return all_data
    

if __name__ == "__main__":
    job = AnomalTransactionDetectionService(chain="0x1")
    token_addresses = ["0xd533a949740bb3306d119cc777fa900ba034cd52"]
    data = job.get_transactions_by_token(token_addresses=token_addresses)
    df = pd.DataFrame(data)
    df.to_csv("anomal_transactions.csv", index=False)