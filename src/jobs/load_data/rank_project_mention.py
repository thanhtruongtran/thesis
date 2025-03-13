import json
import time

import numpy as np
import pandas as pd

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.jobs.cli import CLIJob
from src.utils.list_dict import aggregate_dicts
from src.utils.logger import get_logger
from src.utils.text import detect_language_one_text
from src.utils.time import round_timestamp


class RankProjectMentionJob(CLIJob):
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
        self.logg.info("Start to ranking projects a day ...")

    def _execute(self):
        ## mapping with chain tokens
        self.logg.info("Start to query projects info")
        project_social_cursor = self.klg._db["projects"].find(
            {
                "socialAccounts.twitter": {"$exists": True},
                "deployedChains": {"$exists": True, "$ne": [], "$nin": [[], ["0x0"]]},
            }
        )

        account_mapping_lst = []
        for account in project_social_cursor:
            account_info = {}
            account_id = account["socialAccounts"]["twitter"].split("/")[-1]
            account_info["_id"] = self._remove_at_symbol(account_id).lower()
            account_info["deployedChains"] = account["deployedChains"]
            if "tokenAddresses" in account:
                mentioned_token = []
                for key, value in account["tokenAddresses"].items():
                    _id = f"{key}_{value}"
                    try:
                        mapping_address_token = self.klg.mapping_address_token(_id)
                        if mapping_address_token == "":
                            mentioned_token.append(_id)
                    except Exception:
                        pass
                if len(mentioned_token) > 0:
                    account_info["tokenAddresses"] = mentioned_token

            account_mapping_lst.append(account_info)

        clean_account_mapping_dict = aggregate_dicts(account_mapping_lst, "_id")

        with open("data/mapping_address.json", "w") as file:
            json.dump(clean_account_mapping_dict, file, indent=4)

        self.logg.info(f"Successful query {len(clean_account_mapping_dict)} projects")
        ## ranking mentioned tokens
        pipeline = [
            {
                "$match": {
                    "timestamp": {
                        "$gte": round_timestamp(time.time() - TimeConstants.DAYS_2)
                    },
                    "mentionedProjects": {"$exists": True},
                }
            },
            {"$sort": {"timestamp": -1}},
        ]

        top_projects = MongoDBCDP()._db["tweets"].aggregate(pipeline)

        mention_projects = list()
        for project in top_projects:
            if (
                (len(project["text"]) > 50)
                & ("RT @" not in project["text"])
                & (detect_language_one_text(project["text"]) == "en")
            ):
                for tok in list(set(project["mentionedProjects"])):
                    try:
                        project_df = pd.DataFrame()
                        project_df["account"] = [tok]
                        project_df["text"] = [project["text"]]
                        project_df["views"] = [project["views"]]
                        project_df["mentionTimes"] = [1]
                        mention_projects.append(project_df)
                    except Exception:
                        pass

        mention_df = pd.concat(mention_projects)
        mention_df = mention_df.drop_duplicates(["account", "text"])
        df = (
            mention_df.groupby("account")
            .agg({"views": np.sum, "mentionTimes": np.sum})
            .reset_index()
        )

        df["_id"] = df["account"]
        df["meanView"] = df["views"] / df["mentionTimes"]
        df["meanView"] = df["meanView"].apply(int)
        df["timestamp"] = round_timestamp(
            timestamp=time.time(), round_time=self.interval
        )

        df["deployedChains"] = df.apply(
            lambda x: self._mapping_deployed_chain(
                mapping_dict=clean_account_mapping_dict, row=x
            ),
            axis=1,
        )
        df["tokenSupported"] = df.apply(
            lambda x: self._mapping_tokenSupported_chain(
                mapping_dict=clean_account_mapping_dict, row=x
            ),
            axis=1,
        )

        df["_id"] = df["account"].apply(lambda x: "account_" + str(x))

        df["tags"] = "account"

        update_data = df.to_dict(orient="records")

        format_update_data = [
            {k: v for k, v in row.items() if v not in [0, {}, [], ""]}
            for row in update_data
        ]

        self.mongodb_cdp.update_docs(
            data=format_update_data, collection_name="entity_mentioned_ranking"
        )

    def _mapping_deployed_chain(self, mapping_dict, row):
        try:
            mapping = mapping_dict[row["_id"]]["deployedChains"]
        except Exception:
            mapping = ""

        return mapping

    def _mapping_tokenSupported_chain(self, mapping_dict, row):
        try:
            mapping = mapping_dict[row["_id"]]["tokenAddresses"]
        except Exception:
            mapping = ""

        return mapping

    def _is_error(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
            return 0
        except Exception:
            return 1

    def _remove_at_symbol(self, s):
        return s[1:] if s.startswith("@") else s
