from src.constants.chains import Chains
from src.databases.mongodb_etl import MongoDBETL
from src.utils.logger import get_logger

logger = get_logger("Mapping timestamp and block number!")


def mapping_timestamp_blocknumber(chain_ids):
    multichain_transaction = dict()
    for chain_id in chain_ids:
        chain = Chains.reverse_mapping[chain_id]
        chain_info_update = dict()
        chain_info = dict()

        mongo_transaction = MongoDBETL(chain=chain)

        for (
            start_block,
            start,
        ) in mongo_transaction.get_previous_1week_block().items():
            start_block = start_block
            start = start
        for end_block, end in mongo_transaction.get_largest_block().items():
            end_block = end_block
            end = end

        block_timestamp_mappings = mongo_transaction.get_block_number_to_timestamp(
            start_block=start_block, end_block=end_block
        )
        chain_info["start"] = start
        chain_info["end"] = end
        chain_info["start_block"] = start_block
        chain_info["end_block"] = end_block
        chain_info["block_timestamp_mappings"] = block_timestamp_mappings
        chain_info_update[f"{chain_id}"] = chain_info
        multichain_transaction.update(chain_info_update)

    return multichain_transaction
