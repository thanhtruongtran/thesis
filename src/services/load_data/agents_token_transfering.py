from src.constants.agents import Agents
from src.databases.mongodb_community import MongoDBCommunity
from src.utils.blocks import mapping_timestamp_blocknumber
from src.utils.list_dict import chunk_list
from src.utils.logger import get_logger


class AgentsTokenTransferService:
    def __init__(
        self,
        chain,
        PostgreDB,
        PostgreRagDB,
        batch_size=1200,
    ):
        self.logg = get_logger("Token transfer ")
        self.chain = chain
        self.pg = PostgreDB
        self.pg_rag = PostgreRagDB

        self.logg.info(
            f"Start to get token prices, and mapping between timestamp and blocknumber of chain: {self.chain}..."
        )

        self.token_prices = MongoDBCommunity().query_historical_prices(
            chains=[self.chain]
        )

        self.multichain_transaction = mapping_timestamp_blocknumber(
            chain_ids=[self.chain]
        )
        self.logg.info("Successful !")

        block_list = list(
            self.multichain_transaction[chain]["block_timestamp_mappings"].keys()
        )
        self.sub_block_list = chunk_list(input_list=block_list, chunk_size=batch_size)

    def run(self):
        num_ = len(self.sub_block_list)
        print(f"loading transfer jobs: {num_} jobs")
        loaded_block = []

        for block_list in self.sub_block_list:
            try:
                agent_lst = [
                    agent["address"]
                    for agent in Agents.all_agents
                    if agent["chainId"] == self.chain
                ]

                data = self.pg.query_transfer_events(
                    contract_addresses=agent_lst,
                    from_block=block_list[0],
                    to_block=block_list[-1],
                    block_timestamp_mappings=self.multichain_transaction[self.chain][
                        "block_timestamp_mappings"
                    ],
                    price_history=self.token_prices[self.chain],
                )
                if len(data) > 0:
                    self.pg_rag.insert_transfer_data_to_table(data=data)

                loaded_block.append(block_list)
                num_block = len(loaded_block)
                if num_block % 100 == 0:
                    print(f"loaded {num_block} jobs...")

            except Exception:
                #  print(e)
                continue
