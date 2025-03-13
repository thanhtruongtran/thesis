import os
import sys

sys.path.append(os.getcwd())
import time

from dotenv import load_dotenv

from src.constants.time import TimeConstants
from src.constants.llm.agent_prompt import LeadProjectImpactPromptTemplate, ReFormatPromptTemplate
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_news import MongoDBNews
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

load_dotenv()
logger = get_logger("Lead Projects Impact")


class LeadProjectsImpact:
    def __init__(self):
        self.llm_communication = LLMCommunication()
        self.mongodb_news = MongoDBNews()
        self.mongodb_klg = MongoDBKLG()
        self.mongodb_cdp = MongoDBCDP()
        self.llm = LLMCommunication()

    def get_anomal_tokens(self):
        timestamp = round_timestamp(int(time.time()))
        # token berachain, no idCoinGecko
        cursor_1 = self.mongodb_cdp._db["entity_change_ranking"].find(
            {
                "_id": {"$regex": "0x138de"},
                "priceAnomalPoints": {"$exists": True},
                "recentChangeScore": {"$exists": True},
            }
        )

        # token berachain, idCoinGecko
        cur = self.mongodb_klg._db["smart_contracts"].find(
            {
                "tags": "token_berachain",
                "idCoinGecko": {"$exists": True},
            }
        )
        tokens = list(cur)
        cursor_2 = self.mongodb_cdp._db["entity_change_ranking"].find(
            {
                "idCoinGecko": {"$in": [token["idCoinGecko"] for token in tokens]},
                "priceAnomalPoints": {"$exists": True},
                "recentChangeScore": {"$exists": True},
            }
        )

        list_tokens = list(cursor_1) + list(cursor_2)

        # get tokens have anomal points from 1 previous days, priceAnomalPoints: [timestamp1, timestamp2, timestamp3, ...]
        tokens = []
        for token in list_tokens:
            if str(timestamp - TimeConstants.A_DAY) in token["priceAnomalPoints"]:
                tokens.append(token)

        # sort tokens by recentChangeScore, larger minus value, larger decrease; larger plus value, larger increase
        # get one increase token and one decrease token

        return tokens
    
    def get_anomal_tokens_v2(self):
        timestamp = round_timestamp(int(time.time()))
        
        cursor = self.mongodb_klg._db["smart_contracts"].find(
            {
                "tags": "token_berachain",
                "socialAccounts.twitter": { "$exists": True },
                "priceChangeRate": { "$exists": True }
            }
        )

        # sort by |priceChangeRate|
        tokens = list(cursor)
        tokens = sorted(tokens, key=lambda x: abs(x["priceChangeRate"]), reverse=True)

        # get top 3 tokens (name, symbol, priceChangeRate, priceChangeLogs in 3 previous days)
        # priceChangeLogs is dict of timestamp and price, round timestamp with 3600s
        tokens = tokens[:3]
        for token in tokens:
            da = {}
            for timestamp, price in token["priceChangeLogs"].items():
                if int(float(timestamp)) < round_timestamp(int(time.time())) - TimeConstants.DAYS_3:
                    continue

                if round_timestamp(timestamp, 7200) not in da:
                    da[round_timestamp(timestamp, 7200)] = price
                else:
                    continue
            token["priceChangeLogs"] = da

        tokens = [
            {
                "name": token["name"],
                "symbol": token["symbol"],
                "twitter_username": token["socialAccounts"]["twitter"].split("/")[-1],
                "priceChangeRate": token["priceChangeRate"],
                "priceChangeLogs": token["priceChangeLogs"]
            }
            for token
            in tokens
        ]

        return tokens

    def get_projects_berachain(self):
        cursor_1 = self.mongodb_cdp._db["twitter_users"].find(
            {
                "$or": [
                    {"tags": "project_berachain"},
                    {"tags": "community_berachain"},
                ]
            }
        )
        list_userNames = [user["userName"] for user in cursor_1]

        return list_userNames

    def tweets_of_project_berachain(self, project_userName):
        # get tweets of project_userName from 3 previous days
        cursor_2 = self.mongodb_cdp._db["tweets"].find(
            {
                "authorName": project_userName,
                "timestamp": {
                    "$gte": round_timestamp(int(time.time())) - TimeConstants.DAYS_7
                },
            }
        )

        return list(cursor_2)

    def check_mentioned_tokens(self):
        tokens = self.get_anomal_tokens()
        logger.info(f"Number of tokens: {len(tokens)}")
        projects = self.get_projects_berachain()
        logger.info(f"Number of projects: {len(projects)}")

        project_tweets = {}
        for project in projects:
            tweets = self.tweets_of_project_berachain(project)
            project_tweets[project] = tweets

        # check if project_tweets have mentioned tokens
        mentioned_tokens = {}
        for project, tweets in project_tweets.items():
            for tweet in tweets:
                for token in tokens:
                    if str("$" + token["symbol"]) in tweet["text"].lower():
                        if (project, token["symbol"]) not in mentioned_tokens:
                            mentioned_tokens[(project, token["symbol"])] = [tweet]
                        else:
                            mentioned_tokens[(project, token["symbol"])].append(tweet)

        return mentioned_tokens
    
    def get_tweets_of_anomal_tokens(self, token_userName):
        # get tweets of token from 3 previous days
        cursor = self.mongodb_cdp._db["tweets"].find(
            {
                "authorName": token_userName,
                "timestamp": {
                    "$gte": round_timestamp(int(time.time())) - TimeConstants.DAYS_3
                },
            }
        )

        return list(cursor)
    
    def get_valid_llm_response(
        self, template, info, max_retries=5, max_length=280
    ):
        for attempt in range(max_retries):
            prompt = template.format(
                project_username=info["project_username"],
                twitter_username=info["twitter_username"],
                symbol=info["symbol"],
                list_price=info["list_price"],
                tweets=info["tweets"]
            )

            response = self.llm.send_prompt(prompt)
            if len(response) <= max_length:
                return response

            if attempt == max_retries - 1:
                return None
    
    def create_post(self, info):
        lead_project_impact_template = LeadProjectImpactPromptTemplate()
        template = lead_project_impact_template.create_template()
        
        response = self.get_valid_llm_response(template, info)
        return response
    
    def reformat_post(self, info):
        reformat_template = ReFormatPromptTemplate()
        template = reformat_template.create_template()
        prompt = template.format(
            original_tweet=info["original_tweet"],
        )

        response = self.llm.send_prompt(prompt)
        return response
    
    def lead_project_impact_posting(self):
        tokens = self.get_anomal_tokens_v2()
        logger.info(f"Number of tokens: {len(tokens)}")

        for token in tokens:
            tweets = self.get_tweets_of_anomal_tokens(token["twitter_username"])
            token["tweets"] = tweets

        infos = []
        for token in tokens:
            if token["tweets"] == []:
                continue
            info = {
                "project_username": token["name"],
                "twitter_username": token["twitter_username"],
                "symbol": token["symbol"],
                "list_price": list(token["priceChangeLogs"].values()),
                "tweets": token["tweets"]
            }
            infos.append(info)

        responses = {}
        for info in infos:
            response = self.create_post(info)
            response = self.reformat_post({"original_tweet": response})
            responses[info["project_username"]] = response

        return responses
