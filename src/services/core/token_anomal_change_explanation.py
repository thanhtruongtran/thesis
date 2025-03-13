import os
import sys
import time

sys.path.append(os.getcwd())


from src.constants.chains import Chains
from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_etl import MongoDBETL
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.postgresql import PostgresDB
from src.services.llm.communication import LLMCommunication
from src.utils.time import round_timestamp


class TokenChangeExplainationService:
    def __init__(self, chain_id="0x1"):
        self.llm = LLMCommunication()
        self.chain_id = chain_id
        if self.chain_id == "0x38":
            self.chain = ""
        else:
            self.chain = str(Chains.all_chains.get(chain_id).get("name")).lower()

        self.mongodb_klg = MongoDBKLG()
        self.mongodb_cdp = MongoDBCDP()
        self.mongodb_etl = MongoDBETL(chain=self.chain)
        self.postgres = PostgresDB(chain_id)

    def get_token_change(self):
        # get token change ranking
        timestamp = round_timestamp(time.time() - TimeConstants.A_DAY)
        cursor_1 = (
            self.mongodb_cdp._db["entity_change_ranking"]
            .find(
                {
                    "updateTime": timestamp,
                    "marketCap": {"$exists": True},
                    "_id": {"$regex": f"^{self.chain_id}_"},
                }
            )
            .sort("priceUptodateChangeScore", -1)
        )
        entity_change_ranking = list(cursor_1)

        # get token mention
        timestamp_by_hour = round_timestamp(
            time.time(), round_time=TimeConstants.A_HOUR / 2
        )
        cursor_2 = (
            self.mongodb_cdp._db["token_mentioned_ranking"]
            .find(
                {
                    "timestamp": timestamp_by_hour,
                }
            )
            .sort("mentionTimes", -1)
        )
        lst_token_mention = list(cursor_2)

        # filter token by market cap: lowCap<100M, 100M<=midCap<1B, 1B<=highCap
        for token in entity_change_ranking:
            if token["marketCap"] < 100000000:
                token["marketCapType"] = "lowCap"
            elif 100000000 <= token["marketCap"] < 1000000000:
                token["marketCapType"] = "midCap"
            else:
                token["marketCapType"] = "highCap"

        return entity_change_ranking, lst_token_mention

    def get_transfer_events_of_token(self, token_address):
        start_timestamp = int(time.time() - TimeConstants.DAYS_2)
        # query block number of start timestamp
        block_number = self.mongodb_etl.get_block_number_by_timestamp(start_timestamp)

        # query transfer events of token
        transfer_events = self.postgres.query_transfer_events_of_ca(
            addresses=[token_address], block_number=block_number
        )
        return transfer_events


if __name__ == "__main__":
    token_change_explaination_service = TokenChangeExplainationService()
    # entity_change_ranking, token_mention = token_change_explaination_service.get_token_change()
    # for token in entity_change_ranking:
    #     # print(token['symbol'])
    #     if token['marketCapType'] == 'lowCap' and token['priceUptodateChangeScore'] is not None:
    #         print(token['symbol'])
    #         print(token['priceUptodateChangeScore'])
    #         break

    # print('---------------------')
    # # for token1 in token_mention[50:60]:
    #     print(token1['symbol'])
