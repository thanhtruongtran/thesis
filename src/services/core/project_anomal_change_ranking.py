import pandas as pd
from multithread_processing.base_job import BaseJob

from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.utils.core.change_detection import get_anomaly_columns
from src.utils.logger import get_logger


class ProjectChangeRankingService(BaseJob):
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

        projects = self.klg._db["projects"].find(
            {"tvlByChainsChangeLogs": {"$exists": True}},
            {
                "name": 1,
                "deployedChains": 1,
                "idDefiLlama": 1,
                "socialAccounts": 1,
                "tokenAddresses": 1,
                "tvlByChainsChangeLogs": 1,
                "tvlByChains": 1,
            },
        )
        project_lst = []
        # ranking
        for project in projects:
            project_lst.append(project)

        self.df = pd.DataFrame(project_lst)
        num_projects = len(self.df)

        job_lst = list(range(batch_size, num_projects, batch_size))
        job_lst.append(num_projects)

        super().__init__(
            work_iterable=job_lst, max_workers=max_workers, batch_size=batch_size
        )

    def _execute_batch(self, works):
        updated_projects = []

        for work in works:
            ## filter dataframe for each batch work
            df = self.df.iloc[work - self.batch_size : work].reset_index(drop=True)
            ## tvl

            df_tvl = df.drop(columns=["tokenAddresses"])
            df_tvl = df_tvl.explode("deployedChains")
            df_tvl["tvlByChains"] = df_tvl.apply(
                lambda row: self._mapping_chain(row, "tvlByChains"), axis=1
            )
            df_tvl["tvlByChainsChangeLogs"] = df_tvl.apply(
                lambda row: self._mapping_chain(row, "tvlByChainsChangeLogs"), axis=1
            )

            df_tvl = df_tvl[df_tvl["tvlByChainsChangeLogs"].apply(lambda x: len(x) > 0)]

            df_tvl["tvlVolatility"] = df_tvl["tvlByChainsChangeLogs"].apply(
                lambda row: get_anomaly_columns(row)
            )
            df_tvl["tvlSevendayChangeScore"] = df_tvl["tvlVolatility"].apply(
                lambda row: row[0]
            )
            df_tvl["tvlRecentChangeScore"] = df_tvl["tvlVolatility"].apply(
                lambda row: row[1]
            )
            df_tvl["tvlAnomalPoints"] = df_tvl["tvlVolatility"].apply(
                lambda row: row[2]
            )
            df_tvl["tvlByChainsChangeLogs"] = df_tvl["tvlVolatility"].apply(
                lambda row: row[3]
            )

            df_tvl.drop(columns=["tvlVolatility"], inplace=True)

            df_tvl["recentChangeScore"] = df_tvl["tvlRecentChangeScore"]
            df_tvl["sevendayChangeScore"] = df_tvl["tvlSevendayChangeScore"]

            ## token address
            df_token = df[["_id", "tokenAddresses", "deployedChains"]]
            df_token = df_token.explode("deployedChains")
            df_token["tokenAddresses"] = df_token.apply(
                lambda row: self._mapping_chain(row, "tokenAddresses"), axis=1
            )

            df_token = df_token[df_token["tokenAddresses"].apply(lambda x: len(x) > 0)]

            update_df = pd.merge(
                df_tvl,
                df_token,
                on=["_id", "deployedChains"],
                how="outer",
            )

            update_df = update_df.fillna("")
            update_df["_id"] = update_df.apply(
                lambda row: f"defiLlama-{row['idDefiLlama']}-{row['deployedChains']}",
                axis=1,
            )

            update_df["tags"] = "project"

            format_update_dct = [
                {k: v for k, v in row.items() if v not in [0, {}, [], ""]}
                for row in update_df.to_dict(orient="records")
            ]

            if len(format_update_dct) > 0:
                updated_projects.extend(df._id.tolist())
                self.cdp.update_and_replace_data(
                    collection_name=self.updated_collection, data=format_update_dct
                )

                print(
                    f"Updated {len(updated_projects)} projects / {len(self.df)} projects"
                )

    def _end(self):
        super()._end()
        self.logger.info("End of the job")

    def _mapping_chain(self, row, column):
        try:
            mapping_row = row[column][row["deployedChains"]]

        except Exception:
            mapping_row = {}

        return mapping_row
