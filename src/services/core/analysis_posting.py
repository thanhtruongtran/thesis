import datetime
import os
import sys
import time

sys.path.append(os.getcwd())
from src.constants.llm.agent_prompt import AnalysisPromptTemplate
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_community import MongoDBCommunity
from src.databases.mongodb_klg import MongoDBKLG
from src.services.llm.communication import LLMCommunication


class AnalysisPostingService:
    def __init__(self):
        self.llm = LLMCommunication()
        self.mongodb_community = MongoDBCommunity()
        self.mongodb_cdp = MongoDBCDP()
        self.mongodb_klg = MongoDBKLG()

    def _convert_timestamp(self, timestamp):
        """
        Convert Unix timestamp to human-readable datetime string
        """
        return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    def _format_timeseries_data(self, data):
        """
        Format timeseries data to highlight time elements
        """
        if not data:
            return {}

        formatted_data = {}
        for timestamp, value in data.items():
            formatted_time = self._convert_timestamp(int(timestamp))
            formatted_data[formatted_time] = value
        return formatted_data

    def get_entity_info(self):
        cursor = self.mongodb_cdp._db["entity_change_ranking"].find(
            {
                "$or": [
                    {"recentChangeScore": {"$gt": 1}},
                    {"tvlSevendayChangeScore": {"$gt": 1}},
                ]
            }
        )
        info_lst = list(cursor)

        projects_info = []
        for doc in info_lst:
            if "idDefiLlama" in doc:
                klg_doc = self.mongodb_klg._db["projects"].find_one(
                    {"_id": doc["idDefiLlama"]}
                )
                doc["imgUrl"] = klg_doc.get("imgUrl")
                doc["website"] = klg_doc.get("socialAccounts").get("website")
                projects_info.append(doc)

        tokens_info = []
        for doc in info_lst:
            if "idCoingecko" in doc:
                klg_doc = self.mongodb_klg._db["smart_contracts"].find_one(
                    {"idCoingecko": doc["idCoingecko"]}
                )
                doc["imgUrl"] = klg_doc.get("imgUrl")
                doc["website"] = f"https://bscscan.com/address/{klg_doc['address']}"
                tokens_info.append(doc)

        return projects_info, tokens_info

    def analyze_info(self, info):
        analysis_prompt_template = AnalysisPromptTemplate()
        template = analysis_prompt_template.create_template()

        prompt = template.format(
            entity=info.get("idDefiLlama", info.get("idCoingecko")),
            timeseries_data=info.get("timeseries_data"),
            tag=info.get("tag"),
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_analysis(self):
        list_analysis = []
        projects_info, tokens_info = self.get_entity_info()

        # Process projects
        for project in projects_info:
            tvl_data = project.get("tvlByChainsChangeLogs", {})
            project["timeseries_data"] = {"tvl": self._format_timeseries_data(tvl_data)}
            project["tag"] = "project"
            response = self.analyze_info(project)
            project["analysis"] = response
            project["lastUpdated"] = int(time.time())
            list_analysis.append(project)

        # Process tokens
        for token in tokens_info:
            price_data = token.get("priceChangeLogs", {})
            market_cap_data = token.get("marketCapChangeLogs", {})
            token["timeseries_data"] = {
                "price": self._format_timeseries_data(price_data),
                "market_cap": self._format_timeseries_data(market_cap_data),
            }
            token["tag"] = "token"
            response = self.analyze_info(token)
            token["analysis"] = response
            token["lastUpdated"] = int(time.time())
            list_analysis.append(token)

        return list_analysis
