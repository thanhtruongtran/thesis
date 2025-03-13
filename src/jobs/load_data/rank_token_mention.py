import time

import numpy as np
import pandas as pd

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.jobs.cli import CLIJob
from src.utils.logger import get_logger
from src.utils.text import detect_language_one_text
from src.utils.time import round_timestamp


class RankTokenMentionJob(CLIJob):
    def __init__(
        self,
        interval,
    ):
        super().__init__(interval=interval)
        self.interval = interval
        self.logg = get_logger("Extract entities of Tweets")
        self.mongodb_cdp = MongoDBCDP()
        self.klg = MongoDBKLG()

    def _pre_start(self):
        self.logg.info("Start to ranking tokens a day ...")

    def _execute(self):
        ## mapping with chain tokens
        self.logg.info("Start to query tokens in berachain")
        berachain_cursor = self.klg._db["smart_contracts"].find(
            {"tags": "token_berachain"}
        )

        token_mapping_lst = []
        for token in berachain_cursor:
            token_mapping_lst.append(token["symbol"])

        self.logg.info(f"Successful query {len(token_mapping_lst)} tokens")
        ## ranking mentioned tokens
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": round_timestamp(time.time() - TimeConstants.DAYS_2)
                    },
                    "mentionedTokens": {"$exists": True},
                }
            },
            {"$sort": {"timestamp": -1}},
        ]

        top_token = MongoDBCDP()._db["tweets"].aggregate(pipeline)

        mention_tokens = list()
        for token in top_token:
            if (
                (len(token["text"]) > 50)
                & ("RT @" not in token["text"])
                & (len(token["mentionedTokens"]) <= 3)
                & (detect_language_one_text(token["text"]) == "en")
            ):
                for tok in list(set(token["mentionedTokens"])):
                    if self._is_error(int, tok) == 1:
                        # if tok not in mention_tokens["views"]:
                        try:
                            token_df = pd.DataFrame()
                            token_df["symbol"] = [tok]
                            token_df["text"] = [token["text"]]
                            token_df["views"] = [token["views"]]
                            token_df["mentionTimes"] = [1]
                            mention_tokens.append(token_df)
                        except Exception:
                            pass

        mention_df = pd.concat(mention_tokens)
        mention_df = mention_df.drop_duplicates(["symbol", "text"])
        df = (
            mention_df.groupby("symbol")
            .agg({"views": np.sum, "mentionTimes": np.sum})
            .reset_index()
        )

        df["_id"] = df["symbol"].apply(lambda x: "symbol_" + str(x))
        df["meanView"] = df["views"] / df["mentionTimes"]
        df["meanView"] = df["meanView"].apply(int)
        df["timestamp"] = round_timestamp(
            timestamp=time.time(), round_time=self.interval
        )

        df["categories"] = df.apply(lambda row: self._mapping_category(row), axis=1)
        df["chainId"] = df["symbol"].apply(
            lambda row: "0x138de" if row in token_mapping_lst else ""
        )
        df["tags"] = "symbol"
        update_data = df.to_dict(orient="records")

        self.mongodb_cdp.update_docs(
            data=update_data, collection_name="entity_mentioned_ranking"
        )

    def _is_error(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
            return 0
        except Exception:
            return 1

    def _mapping_category(self, row):
        categories = []
        token_cursor = (
            MongoDBCDP()
            ._db["entity_change_ranking"]
            .find({"symbol": row["symbol"]}, {"symbol", "categories"})
        )
        for category in token_cursor:
            if "categories" in category:
                categories.extend(category["categories"])

        return categories
