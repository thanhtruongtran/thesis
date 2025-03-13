import os
import sys

sys.path.append(os.getcwd())
import time
from datetime import datetime, timedelta

import numpy as np
import requests
from dotenv import load_dotenv

from src.constants.llm.agent_prompt import (
    BerachainNewsPromptTemplate,
    LendingNewsPromptTemplate,
    NewsPromptTemplate,
    ReplyNewsPromptTemplate,
    TokenNewsPromptTemplate,
)
from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_klg import MongoDBKLG
from src.databases.mongodb_news import MongoDBNews
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger
from src.utils.text import data_process

load_dotenv()
logger = get_logger("News Posting Service")


class NewsPostingService:
    def __init__(self, symbol=None):
        self.mongodb_news = MongoDBNews()
        self.mongodb_cdp = MongoDBCDP()
        self.mongodb_klg = MongoDBKLG()
        self.llm = LLMCommunication()
        self.lending_base_api = os.getenv("API_LENDING_NEWS")
        self.berachain_news_api = os.getenv("API_BERACHAIN_NEWS")
        self.symbol = symbol

    def get_data(self, start_time, end_time, interval):
        return self.mongodb_news.get_keywords_data(start_time, end_time, interval)

    def get_lending_data(self):
        # Get list of lending pool names that were mentioned in the last 7 days, ranked by the number of mentions
        list_lending_pools = requests.get(
            self.lending_base_api + "/lending/get_top_lending_pools/"
        ).json()

        # Get the summary of the input lending pool name with its basic information
        lending_pools_summary = []
        for pool in list_lending_pools:
            pool_summary = requests.get(
                self.lending_base_api + f"/{pool}/get_lending_summary/"
            ).json()
            lending_pools_summary.append(pool_summary)

        return lending_pools_summary

    def get_berachain_news_data(self):
        start_time = datetime.strftime(datetime.now() - timedelta(days=1), "%Y-%m-%d")
        end_time = datetime.strftime(datetime.now(), "%Y-%m-%d")
        berachain_news = requests.get(self.berachain_news_api + f"/berachain/search_keyword_with_timestamp?start_time={start_time}&end_time={end_time}&limit=40").json()
        return berachain_news

    def compare_similarity(self, keyword_summary1, keyword_summary2):
        keyword_summary1 = data_process(keyword_summary1)[0]
        keyword_summary2 = data_process(keyword_summary2)[0]

        embedding1 = self.llm.embedding(keyword_summary1)
        embedding2 = self.llm.embedding(keyword_summary2)
        embedding1 = np.array(embedding1)
        embedding2 = np.array(embedding2)

        cosine_similarity = np.dot(embedding1, embedding2) / (
            np.linalg.norm(embedding1) * np.linalg.norm(embedding2)
        )

        return cosine_similarity

    def check_keyword_in_post(self, keyword, post):
        keyword = keyword.lower()
        post = post.lower()
        if keyword in post:
            return True
        return False

    def post_news(self, info):
        news_prompt_template = NewsPromptTemplate()
        template = news_prompt_template.create_template()
        for i in range(len(info.get("summary"))):
            if "[Sources:" in info.get("summary")[i]:
                info.get("summary")[i] = (
                    info.get("summary")[i]
                    .split("[Sources:")[0]
                    .replace("•", "")
                    .strip()
                )

        prompt = template.format(
            information_keyword=info.get("keyword"),
            information_description=info.get("description"),
            information_summary=info.get("summary")[: min(2, len(info.get("summary")))],
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_lending_news(self, info):
        lending_news_prompt_template = LendingNewsPromptTemplate()
        template = lending_news_prompt_template.create_template()
        for i in range(len(info.get("summary"))):
            if "[Sources:" in info.get("summary")[i]:
                info.get("summary")[i] = (
                    info.get("summary")[i]
                    .split("[Sources:")[0]
                    .replace("•", "")
                    .strip()
                )

        prompt = template.format(
            information_symbol=info.get("symbol"),
            information_main_event=info.get("main_event"),
            information_summary=info.get("summary")[: min(3, len(info.get("summary")))],
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_reply_news(self, info):
        reply_news_prompt_template = ReplyNewsPromptTemplate()
        template = reply_news_prompt_template.create_template()
        prompt = template.format(
            information_keyword=info.get("keyword"),
            information_tweet=info.get("tweet"),
            information_tokens_affected=info.get("tokens_affected"),
            information_summary=info.get("summary"),
            information_sentiment=info.get("sentiment"),
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_news_daily(self, isContent=True):
        # Get data yesterday
        start_time = int(time.time() - TimeConstants.A_DAY)
        end_time = int(time.time())
        interval = "1d"
        data_yesterday = self.get_data(start_time, end_time, interval)
        data_yesterday = sorted(
            data_yesterday, key=lambda x: x.get("timestamp"), reverse=True
        )[0]

        # Get data last 2 days
        start_time = int(time.time() - TimeConstants.DAYS_2)
        end_time = int(time.time() - TimeConstants.A_DAY)
        interval = "1d"
        data_last_2_days = self.get_data(start_time, end_time, interval)
        data_last_2_days = sorted(
            data_last_2_days, key=lambda x: x.get("timestamp"), reverse=True
        )[0]

        # Compare each keyword in yesterday data with last 2 days data to filter top 3 keywords for posting
        keywords_yesterday = [
            info.get("keyword") for info in data_yesterday.get("data")
        ]
        keywords_last_2_days = [
            info.get("keyword") for info in data_last_2_days.get("data")
        ]
        for keyword1, keyword2 in zip(keywords_yesterday, keywords_last_2_days):
            if keyword1 != keyword2:
                continue

            keyword_summary1 = data_yesterday.get("data")[
                keywords_yesterday.index(keyword1)
            ].get("summary")
            keyword_summary2 = data_last_2_days.get("data")[
                keywords_last_2_days.index(keyword2)
            ].get("summary")
            keyword_summary1 = ". ".join(keyword_summary1)
            keyword_summary2 = ". ".join(keyword_summary2)
            similarity = self.compare_similarity(keyword_summary1, keyword_summary2)
            if similarity > 0.8:
                keywords_yesterday.remove(keyword1)

        kw_posted_info = self.mongodb_cdp._db["agent_contents"].find(
            {
                "lastUpdated": {"$gte": round(time.time() - TimeConstants.DAYS_7)},
                "isPosted": True,
            },
            {"keyWord": 1},
        )

        kw_posted_lst = []
        for kw_posted in kw_posted_info:
            kw_posted_lst.append(kw_posted["keyWord"])

        keywords_for_postings = []
        for yester_kw in keywords_yesterday:
            if yester_kw not in kw_posted_lst:
                keywords_for_postings.append(yester_kw)

        keywords_for_posting = keywords_for_postings[:-1]

        information_for_posting = []
        for keyword in keywords_for_posting:
            for info in data_yesterday.get("data"):
                if info.get("keyword") == keyword:
                    information_for_posting.append(info)
                    break

        lst_posts = []
        for info in information_for_posting:
            response = self.post_news(info)
            lst_posts.append(response)

        results_news = {}
        for keyword, post in zip(keywords_for_posting, lst_posts):
            results_news[keyword] = post

        # check if a keyword is in post of another keyword
        updated_results_news = results_news.copy()
        for keyword, post in updated_results_news.items():
            for kw in keywords_for_posting:
                if keyword == kw:
                    continue

                if self.check_keyword_in_post(kw, post):
                    try:
                        results_news.pop(kw)
                    except KeyError:
                        pass

        # check if a token in a post is in another post in the last 7 days, if yes, not consider this post
        cursor = self.mongodb_cdp._db["agent_contents"].find(
            {
                "lastUpdated": {"$gte": round(time.time() - TimeConstants.DAYS_7)},
                "isPosted": True,
                "type": "news",
                "keyWord": keyword,
            },
        )
        updated_results_news_2 = results_news.copy()
        for keyword, post in updated_results_news_2.items():
            lst_words = post.split()
            for word in lst_words:
                if "$" in word:
                    for doc in cursor:
                        if word in doc.get("content"):
                            try:
                                results_news.pop(keyword)
                            except KeyError:
                                pass

        # get top 2 keywords for posting
        results_news_processed = dict(list(results_news.items())[:2])
        keywords_for_posting = list(results_news_processed.keys())

        # Generate reply news
        # get tokens_affected in last 3 days data

        information_for_replys = []
        for kw, tweet in results_news_processed.items():
            cursor = self.mongodb_cdp._db["agent_contents"].find(
                {
                    "lastUpdated": {"$gte": round(time.time() - TimeConstants.DAYS_7)},
                    "isPosted": True,
                    "type": "news",
                    "keyWord": kw,
                },
                {"repliesContent.tokensAffected": 1},
            )
            tokens_affected_last_7_days = []
            for doc in cursor:
                tokens_affected_last_7_days.extend(
                    doc.get("repliesContent").get("tokensAffected")
                )
            tokens_affected_last_7_days = list(set(tokens_affected_last_7_days))

            info = {}
            info["keyword"] = kw
            info["tweet"] = tweet
            info["tokens_affected"] = []
            info["summary"] = []
            info["sentiment"] = []
            tokens_affected = data_yesterday.get("data")[
                keywords_yesterday.index(kw)
            ].get("tokens_affected")
            if not tokens_affected:
                continue

            for token in tokens_affected:
                if len(info["tokens_affected"]) == 3:
                    break
                if token.get("name").lower() == kw.lower():
                    continue
                if (
                    token.get("name").lower() == "bitcoin"
                    or token.get("name").lower() == "ethereum"
                ):
                    continue
                if token.get("symbol") in tokens_affected_last_7_days:
                    continue

                info["tokens_affected"].append(token.get("symbol"))
                info["summary"].append(token.get("summary"))
                info["sentiment"].append(token.get("sentiment"))
                information_for_replys.append(info)

            if len(info["tokens_affected"]) < 3:
                for token in tokens_affected:
                    if len(info["tokens_affected"]) == 3:
                        break
                    if token.get("name") in info["tokens_affected"]:
                        continue
                    if (
                        token.get("name").lower() == "bitcoin"
                        or token.get("name").lower() == "ethereum"
                    ):
                        continue

                    info["tokens_affected"].append(token.get("symbol"))
                    info["summary"].append(token.get("summary"))
                    info["sentiment"].append(token.get("sentiment"))
                    information_for_replys.append(info)

        if isContent is True:
            lst_replys = []
            for info in information_for_replys:
                if not info.get("tokens_affected"):
                    continue

                response = self.post_reply_news(info)
                response = self.llm.post_process(response)
                lst_replys.append(response)

            results_reply_news = {}

            lst_tokens_affected = [
                info.get("tokens_affected") for info in information_for_replys
            ]
            for keyword, token, reply in zip(
                keywords_for_posting, lst_tokens_affected, lst_replys
            ):
                results_reply_news[keyword] = {
                    "tokensAffected": token,
                    "replies": reply,
                }

            return results_news_processed, results_reply_news

        else:
            return information_for_posting, information_for_replys

    def post_lending_news_daily(self):
        lending_pools_summary = self.get_lending_data()

        # get lending news in the last 3 days
        cursor = self.mongodb_cdp._db["agent_lending/borrowing_contents"].find(
            {
                "lastUpdated": {"$gte": round(time.time() - TimeConstants.DAYS_3)},
                "isPosted": True,
                "type": "lending_news",
            },
            {"symbol": 1},
        )
        posted_symbols = list(set([doc.get("symbol") for doc in cursor]))

        # get lending pools that have not been posted in the last 3 days
        lending_pools_for_posting = []
        for pool in lending_pools_summary:
            if pool.get("symbol") not in posted_symbols:
                lending_pools_for_posting.append(pool)

        if len(lending_pools_for_posting) == 0:
            return {}

        lending_pools_for_posting = lending_pools_for_posting[
            : min(2, len(lending_pools_for_posting))
        ]

        lst_posts = []
        for pool in lending_pools_for_posting:
            response = self.post_lending_news(pool)
            lst_posts.append(response)

        results_news = {}
        for pool, post in zip(lending_pools_for_posting, lst_posts):
            results_news[pool.get("symbol")] = post

        return results_news

    def post_token_news(self, info):
        prompt_template = TokenNewsPromptTemplate()
        template = prompt_template.create_template()
        news_processed = []
        for news in info.get("news"):
            if "[Sources:" in news:
                news = news.split("[Sources:")[0].replace("•", "").strip()
            news_processed.append(news)

        prompt = template.format(symbol=info.get("symbol"), news=news_processed)
        response = self.llm.send_prompt(prompt)
        return response

    def post_token_news_daily(self):
        # Get data yesterday
        start_time = int(time.time() - TimeConstants.A_DAY)
        end_time = int(time.time())
        interval = "1d"
        data_yesterday = self.get_data(start_time, end_time, interval)
        data_yesterday = sorted(
            data_yesterday, key=lambda x: x.get("timestamp"), reverse=True
        )[0]

        # check if symbol is in the last day news
        last_days_news = []
        for info in data_yesterday.get("data"):
            last_days_news.append(info.get("summary"))

        # get name of the symbol
        list_name_of_symbol = self.mongodb_klg.get_name_of_symbol(
            self.symbol, "smart_contracts"
        )

        news_for_posting = []
        for info in last_days_news:
            for inf in info:
                if self.symbol.lower() in inf.lower():
                    news_for_posting.append(inf)

        if len(news_for_posting) == 0:
            for name in list_name_of_symbol:
                for inf in info:
                    if name.lower() in inf.lower():
                        news_for_posting.append(inf)

        news_for_posting = list(set(news_for_posting))[:3]

        # post news
        info_for_posting = {"symbol": self.symbol, "news": news_for_posting}
        resutls = {
            "symbol": self.symbol,
            "content": self.post_token_news(info_for_posting),
        }
        return resutls

    def post_berachain_news(self, info):
        prompt_template = BerachainNewsPromptTemplate()
        template = prompt_template.create_template()

        prompt = template.format(
            information_content=info.get("content"),
        )

        response = self.llm.send_prompt(prompt)
        return response

    def post_berachain_news_daily(self):
        berachain_news = self.get_berachain_news_data()
        if len(berachain_news) == 0:
            return {}
        
        # compare similarity between 2 each news, select 2 news that have the lowest similarity, using "doc_embeddings" in each news
        berachain_news_for_posting = []
        similarity_score = {}
        for i in range(len(berachain_news)):
            for j in range(i + 1, len(berachain_news)):
                embedding1 = berachain_news[i].get("doc_embeddings")
                embedding2 = berachain_news[j].get("doc_embeddings")
                cosine_similarity = np.dot(embedding1, embedding2) / (
                    np.linalg.norm(embedding1) * np.linalg.norm(embedding2)
                )
                similarity_score[(i, j)] = cosine_similarity

        sorted_similarity_score = sorted(similarity_score.items(), key=lambda x: x[1])
        idxs = sorted_similarity_score[0][0]
        berachain_news_for_posting.append(berachain_news[idxs[0]])
        berachain_news_for_posting.append(berachain_news[idxs[1]])

        lst_posts = []
        for news in berachain_news_for_posting:
            response = self.post_berachain_news(news)
            lst_posts.append(response)

        results_news = {}
        for news, post in zip(berachain_news_for_posting, lst_posts):
            results_news[news.get("title")] = post

        return results_news
