import time
from datetime import datetime

from src.constants.llm.agent_prompt import (
    TrendingAgentTokenPostPromptTemplate,
)
from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_community import MongoDBCommunity
from src.databases.mongodb_news import MongoDBNews
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger
from src.utils.text import data_process, detect_language_one_text

logger = get_logger("Trending Tokens Posting Service")


class TrendingAIAgentTokensPostingService:
    def __init__(self):
        self.mongodb_news = MongoDBNews()
        self.mongodb_cdp = MongoDBCDP()
        self.mongodb_community = MongoDBCommunity()
        self.llm = LLMCommunication()
        self.chain_mapping = {
            int(1): "Ethereum",
            int(-2): "Solana",
            int(8453): "Base",
            int(43114): "Avax",
            int(42161): "Kima",
            int(56): "BSC",
        }

    def get_tokens_from_community(self):
        list_tokens = []
        cursor1 = self.mongodb_community._db["cookiefun_agents"].find(
            {"agentDetails.contracts.contractAddress": {"$exists": True}}
        )
        for da in cursor1:
            try:
                # get rawDescription from twitter_users in cdp
                twitterUsername = da.get("agentDetails", {}).get(
                    "twitterUsernames", []
                )[0]
                cur = self.mongodb_cdp._db["twitter_users"].find_one(
                    {"userName": twitterUsername}
                )
                rawDescription = cur.get("rawDescription") if cur else " "

                list_tokens.append(
                    {
                        "chain": self.chain_mapping.get(
                            int(
                                da.get("agentDetails", {})
                                .get("contracts", {})
                                .get("chain")
                            )
                        ),
                        "contractAddress": da.get("agentDetails", {})
                        .get("contracts", {})
                        .get("contractAddress", " "),
                        "symbol": da.get("ticker", ""),
                        "twitterUsername": da.get("agentDetails", {}).get(
                            "twitterUsernames", []
                        )[0],
                        "createdAt": int(da.get("creationTimestamp")),
                        "marketCap": list(da.get("marketCapGraph").items())[-1][1],
                        "description": rawDescription,
                    }
                )
            except Exception:
                pass

        cursor2 = self.mongodb_community._db["aigentfi_agents"].find({})
        for da in cursor2:
            try:
                list_tokens.append(
                    {
                        "chain": "zkSync",
                        "contractAddress": da.get("contractAddress", " "),
                        "symbol": da.get("symbol", " "),
                        "twitterUsername": da.get("twitterLink").split("/")[-1],
                        "createdAt": int(
                            datetime.strptime(
                                da.get("createdAt"), "%Y-%m-%dT%H:%M:%S.%fZ"
                            ).timestamp()
                        ),
                        "marketCap": da.get("marketCap", 0),
                        "description": da.get("description", " "),
                    }
                )
            except Exception:
                pass

        return list_tokens

    def get_tweets_from_cdp(self):
        pipeline = [
            {
                "$match": {
                    "timestamp": {"$gte": int(time.time() - TimeConstants.A_DAY)},
                    "mentionedTokens": {"$exists": True},
                    # "keyWords": {"$exists": True},
                },
            },
            {"$sort": {"timestamp": -1}},
            {"$limit": 10000},
            {
                "$project": {
                    "_id": 1,
                    "author": 1,
                    "authorName": 1,
                    "timestamp": 1,
                    "text": 1,
                    "views": 1,
                    "mentionedTokens": 1,
                    "keyWords": 1,
                }
            },
        ]
        cursor = self.mongodb_cdp._db["tweets"].aggregate(pipeline)
        tweets = []
        for tweet in cursor:
            tweets.append(tweet)

        return tweets

    def get_tweets_from_cdp_by_token(self, token_twitter_username):
        pipeline = [
            {
                "$match": {
                    "authorName": token_twitter_username,
                    "timestamp": {"$gte": int(time.time() - TimeConstants.DAYS_7 * 2)},
                },
            },
            {"$sort": {"timestamp": -1}},
            {
                "$project": {
                    "_id": 1,
                    "author": 1,
                    "authorName": 1,
                    "timestamp": 1,
                    "text": 1,
                    "views": 1,
                    "mentionedTokens": 1,
                    "keyWords": 1,
                }
            },
        ]
        cursor = self.mongodb_cdp._db["tweets"].aggregate(pipeline)
        tweets = []
        for tweet in cursor:
            tweets.append(tweet)

        return tweets

    def get_tweets_by_token(self, token, tweets):
        # get tweets if token is in "mentionedTokens" field (list of tokens mentioned in the tweet)
        tweets_by_token = []
        for tweet in tweets:
            if token in tweet.get("mentionedTokens"):
                tweets_by_token.append(tweet)

        return tweets_by_token

    def get_top_new_aiagent_tokens(self):
        list_tokens = self.get_tokens_from_community()
        tweets = self.get_tweets_from_cdp()

        # get tokens created in the last 4 weeks, have tweets
        list_tokens = [
            token
            for token in list_tokens
            if int(token.get("createdAt")) > int(time.time() - TimeConstants.DAYS_7 * 4)
        ]
        list_tokens = [
            token
            for token in list_tokens
            if len(self.get_tweets_from_cdp_by_token(token.get("twitterUsername"))) > 0
        ]

        # filter out tokens that have been posted in the last one week
        cur = self.mongodb_cdp._db["agent_contents"].find(
            {
                "type": "news_aiagent_tokens",
                "lastUpdated": {"$gte": int(time.time() - TimeConstants.DAYS_7)},
            }
        )
        list_tokens_posted = []
        for da in cur:
            list_tokens_posted.append(da.get("keyWord"))

        list_tokens = [
            token
            for token in list_tokens
            if token.get("symbol") not in list_tokens_posted
        ]

        # get top 10 tokens that have highest market cap
        list_tokens = sorted(
            list_tokens, key=lambda x: x.get("marketCap"), reverse=True
        )[:10]

        # get top 2 tokens that mentioned the most in tweets
        list_symbols = []
        for token in list_tokens:
            list_symbols.append(token.get("symbol").lower())
        cur = self.mongodb_cdp._db["token_mentioned_ranking"].find(
            {"_id": {"$in": list_symbols}}
        )
        token_mentioned_ranking = {}
        for da in cur:
            token_mentioned_ranking[da.get("_id")] = da.get("mentionTimes")

        list_tokens = sorted(
            list_tokens,
            key=lambda x: token_mentioned_ranking.get(x.get("symbol").lower(), 0),
            reverse=True,
        )[:5]

        # check tweets that mention the token in list_tokens
        token_tweets = {}
        for token in list_tokens:
            tweets_by_token = self.get_tweets_by_token(
                token.get("symbol").lower(), tweets
            )
            key = f"{token.get('symbol')} | {token.get('chain')} | {token.get('contractAddress')} | {token.get('description')}"
            token_tweets[key] = tweets_by_token

        return token_tweets

    def get_valid_llm_response(
        self, template, info, tweets, max_retries=5, max_length=280
    ):
        for attempt in range(max_retries):
            prompt = template.format(
                symbol=info.get("symbol"),
                chain=info.get("chain"),
                contract_address=info.get("contractAddress"),
                description=info.get("description"),
                tweets=tweets,
            )

            response = self.llm.send_prompt(prompt)
            if len(response) <= max_length:
                return response

            if attempt == max_retries - 1:
                return None

    def post_trending_aiagent_tokens(self):
        trending_agent_token_post_prompt_template = (
            TrendingAgentTokenPostPromptTemplate()
        )
        template = trending_agent_token_post_prompt_template.create_template()

        token_tweets = self.get_top_new_aiagent_tokens()
        results = {}
        for key, tweets in token_tweets.items():
            # filter out tweets that are not in English
            tweets = [
                tweet
                for tweet in tweets
                if detect_language_one_text(tweet.get("text")) == "en"
            ]
            # get 5 tweets have the most views
            tweets = sorted(tweets, key=lambda x: x.get("views", 0), reverse=True)[
                : min(5, len(tweets))
            ]
            info = {
                "symbol": key.split(" | ")[0],
                "chain": key.split(" | ")[1],
                "contractAddress": key.split(" | ")[2],
                "description": key.split(" | ")[3],
                "tweets": [data_process(tweet.get("text"))[0] for tweet in tweets],
            }

            response = self.get_valid_llm_response(template, info, info.get("tweets"))
            if len(results) == 2:
                break

            if response:
                results[
                    f"{info.get('symbol')} | {info.get('chain')} | {info.get('contractAddress')}"
                ] = response
            else:
                continue

        return results
