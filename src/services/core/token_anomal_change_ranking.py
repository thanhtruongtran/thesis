import time

import pandas as pd
from IPython.utils import io
from multithread_processing.base_job import BaseJob

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.services.api.centic_api import CenticApi
from src.utils.core.change_detection import get_anomaly_columns
from src.utils.list_dict import divide_into_sublists
from src.utils.logger import get_logger
from src.utils.text import contains_text_or_icon
from src.utils.time import round_timestamp


class TokenChangeRankingService(BaseJob):
    def __init__(
        self,
        updated_collection,
        max_workers=4,
        batch_size=1000,
    ):
        self.logger = get_logger("Update data")
        self.klg = MongoDBKLG()
        self.cdp = MongoDBCDP()
        self.batch_size = batch_size
        self.updated_collection = updated_collection
        self.mapping_project = dict()
        tokens = self.klg._db["smart_contracts"].find(
            {
                "$or": [
                    {"idCoingecko": {"$exists": True}},
                    {"tags": "token_berachain"},
                ],
                "priceChangeLogs": {"$exists": True},
            },
            {
                "symbol": 1,
                "name": 1,
                "price": 1,
                "marketCap": 1,
                "priceChangeLogs": 1,
                "marketCapChangeLogs": 1,
                "idCoingecko": 1,
            },
        )
        token_lst = []
        # ranking
        for token in tokens:
            token_lst.append(token)

        dff = pd.DataFrame(token_lst)
        dff["checkSymbol"] = dff["symbol"].apply(lambda row: contains_text_or_icon(row))
        dff = dff[dff["checkSymbol"] == 1].reset_index(drop=True)
        dff.drop(columns=["checkSymbol"], inplace=True)
        self.df = dff.copy()
        self.df["idCoingecko"] = self.df["idCoingecko"].fillna("")
        symbol_lst = self.df.symbol.unique().tolist()
        job_lst = divide_into_sublists(lst=symbol_lst, n=batch_size)

        super().__init__(
            work_iterable=job_lst,
            max_workers=max_workers,
            batch_size=int(len(job_lst) / batch_size),
        )

    def _execute_batch(self, works):
        updated_tokens = []
        for work in works:
            ## filter dataframe for each batch work
            with io.capture_output() as captured:
                df = self.df[self.df["symbol"].isin(work)].reset_index(drop=True)

                df["_id"] = df.apply(
                    lambda row: "coingecko-" + row["idCoingecko"]
                    if row["idCoingecko"] != ""
                    else row["_id"],
                    axis=1,
                )

                ## fill na for marketCap, marketCapChangeLogs with token do not have idCoingecko
                df.loc[df["_id"].apply(lambda x: bool("_0x" in x)), "marketCap"] = (
                    df.loc[
                        df["_id"].apply(lambda x: bool("_0x" in x)), "marketCap"
                    ].fillna(0)
                )

                df.loc[
                    df["_id"].apply(lambda x: bool("_0x" in x)), "marketCapChangeLogs"
                ] = df.loc[
                    df["_id"].apply(lambda x: bool("_0x" in x)), "marketCapChangeLogs"
                ].fillna(0)

                df.drop_duplicates(subset=["_id"], keep="first", inplace=True)

                df = df.dropna().reset_index(drop=True)

                df["updateTime"] = df["priceChangeLogs"].apply(
                    lambda x: round_timestamp(int(max(x)))
                )
                df["priceVolatility"] = df["priceChangeLogs"].apply(
                    lambda row: get_anomaly_columns(row)
                )
                df["priceSevendayChangeScore"] = df["priceVolatility"].apply(
                    lambda row: row[0]
                )
                df["priceRecentChangeScore"] = df["priceVolatility"].apply(
                    lambda row: row[1]
                )
                df["priceAnomalPoints"] = df["priceVolatility"].apply(
                    lambda row: row[2]
                )
                df["priceChangeLogs"] = df["priceVolatility"].apply(lambda row: row[3])
                df.drop(columns=["priceVolatility"], inplace=True)

                df["marketCapVolatility"] = df["marketCapChangeLogs"].apply(
                    lambda row: get_anomaly_columns(row)
                )
                df["marketCapSevendayChangeScore"] = df["marketCapVolatility"].apply(
                    lambda row: row[0]
                )
                df["marketCapRecentChangeScore"] = df["marketCapVolatility"].apply(
                    lambda row: row[1]
                )
                df["marketCapAnomalPoints"] = df["marketCapVolatility"].apply(
                    lambda row: row[2]
                )
                df["marketCapChangeLogs"] = df["marketCapVolatility"].apply(
                    lambda row: row[3]
                )

                df.drop(columns=["marketCapVolatility"], inplace=True)

                df["recentChangeScore"] = (
                    df["priceRecentChangeScore"] + df["marketCapRecentChangeScore"]
                )
                df["sevendayChangeScore"] = (
                    df["priceSevendayChangeScore"] + df["marketCapSevendayChangeScore"]
                )

                df["tags"] = "token"

                update_dct = df.to_dict(orient="records")

                format_update_dct = [
                    {k: v for k, v in row.items() if v not in [0, {}, [], ""]}
                    for row in update_dct
                ]

            if len(format_update_dct) > 0:
                updated_tokens.extend(df.symbol.tolist())
                self.cdp.update_and_replace_data(
                    collection_name=self.updated_collection, data=format_update_dct
                )
                # self.cdp.update_docs(
                #     collection_name=self.updated_collection,
                #     data=format_update_dct,
                #     merge=False,
                # )
                print(f"Updated {len(updated_tokens)} tokens / {len(self.df)} tokens")

    def _end(self):
        super()._end()
        self.logger.info("End of the job")

    def _mapping(self, row):
        if row in self.mapping_project:
            return self.mapping_project[row]
        else:
            return ""

    def _get_centic_data(self, row):
        if row == "":
            return {}, {}, {}
        else:
            centic_api = CenticApi(
                prj_name=row,
                start=round_timestamp(time.time() - TimeConstants.DAYS_7),
                end=round_timestamp(time.time()),
            )
            try:
                tvl = centic_api.get_tvl()
                tvl_dict = dict(zip(tvl["timestamp"].apply(str), tvl["tvl"]))
            except Exception:
                tvl_dict = {}

            try:
                active_users = centic_api.get_unique_active_user()
                active_users_dict = dict(
                    zip(
                        active_users["timestamp"].apply(str),
                        active_users["numberOfUsers"],
                    )
                )
            except Exception:
                active_users_dict = {}

            try:
                holders = centic_api.get_wallet_holders()
                active_holders_dict = dict(
                    zip(holders["timestamp"].apply(str), holders["activeUsers"])
                )
            except Exception:
                active_holders_dict = {}

            return tvl_dict, active_users_dict, active_holders_dict
