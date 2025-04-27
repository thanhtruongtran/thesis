import time

from cli_scheduler.scheduler_job import SchedulerJob

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.services.core.analysis_posting import AnalysisPostingService
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger
from src.utils.time import round_timestamp


class PostAnalysisJob(SchedulerJob):
    def __init__(
        self,
        interval,
        delay,
        run_now,
        category="",
        chain_id="all",
        keyword="",
    ):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.llm = LLMCommunication()
        self.cdp = MongoDBCDP()
        self.category = category
        self.keyword = keyword
        self.interval = interval
        self.chain_id = chain_id

    def _execute(self, *args, **kwargs):
        posted_token_lst = self._get_posted_tokens(category=self.category)
        if self.keyword != "":
            logger = get_logger(f"Analysis Post Job for keyword: {self.keyword}")

            self.get_keyword_summary_post()

            logger.info("Post Analysis Job finished !")

        else:
            if self.category == "":
                top_mentioned_tokens = self._get_top_mentioned_tokens(
                    chain_id=self.chain_id
                )
                logger = get_logger("Analysis Post Job")

                logger.info("period analysis posting")
                self.get_period_analysis_post(
                    posted_token_lst=posted_token_lst,
                    top_mentioned_tokens=top_mentioned_tokens,
                    chain_id=self.chain_id,
                )

            else:
                top_category_mentioned_tokens = (
                    self._get_top_category_mentioned_tokens()
                )
                print(top_category_mentioned_tokens)
                logger = get_logger(f"Analysis {self.category} Post Job")
                if (
                    round_timestamp(timestamp=time.time(), round_time=43200) % 86400
                    == 0
                ):  ## if round(UTC) = 0h
                    logger.info(f"posting {self.category} token posts")
                    isPosted = self.get_period_category_analysis_post(
                        posted_token_lst=posted_token_lst,
                        top_category_mentioned_tokens=top_category_mentioned_tokens,
                    )
                    if isPosted is False:
                        self.get_recent_category_analysis_post(
                            posted_token_lst=posted_token_lst,
                            top_category_mentioned_tokens=top_category_mentioned_tokens,
                        )

                else:  ## if round(UTC) = 12
                    logger.info("Recent anomal analysis started")
                    self.get_recent_category_analysis_post(
                        posted_token_lst=posted_token_lst,
                        top_category_mentioned_tokens=top_category_mentioned_tokens,
                    )

                logger.info("Post Analysis Job finished !")

    def get_recent_analysis_post(self, posted_token_lst, top_mentioned_tokens):
        pipeline = [
            {
                "$match": {
                    "updateTime": {"$gte": round_timestamp(time.time() - 86400)},
                }
            },
            {"$sort": {"recentChangeScore": -1}},
        ]

        top_token = self.cdp._db["entity_change_ranking"].aggregate(pipeline)

        update_lst = []
        used_materials = []
        for token_info in top_token:
            if len(update_lst) >= 5:  # Tăng lên 5 bài mỗi lần
                break

            if (token_info["symbol"] not in posted_token_lst) & (
                token_info["symbol"] in top_mentioned_tokens
            ):
                analysis = AnalysisPostingService(token_info=token_info)
                response, timeseries_data = analysis.get_recent_analysis_post()
                if response != {}:
                    print(f"token posted: {token_info['symbol']}")
                    _id = token_info["symbol"] + "_" + str(round_timestamp(time.time()))
                    update_lst.append(
                        self._get_update_info(
                            _id=_id,
                            symbol=token_info["symbol"],
                            response=response,
                            chain_id=token_info.get("chainId"),
                        ),
                    )

                    used_materials.append(
                        self._used_materials(_id=_id, timeseries_data=timeseries_data)
                    )

        print(f"num recent posts: {len(update_lst)}")

        self.cdp.update_docs(
            collection_name="agent_contents", data=update_lst, merge=True
        )

        self.cdp.update_docs(
            collection_name="agent_used_documents",
            data=used_materials,
            merge=True,
        )

    def get_period_analysis_post(
        self, posted_token_lst, top_mentioned_tokens, chain_id
    ):
        if chain_id == "all":
            top_50_token_pipeline = [
                {
                    "$match": {
                        "updateTime": {"$gte": round_timestamp(time.time() - 86400)},
                        "marketCap": {"$gte": 500000},
                    }
                },
                {"$sort": {"marketCapSevendayChangeScore": -1}},
                {"$limit": 50},
            ]

            top_50_token_cursor = self.cdp._db["entity_change_ranking"].aggregate(
                top_50_token_pipeline
            )

            top_50_token_change_cursor = [
                token["symbol"] for token in top_50_token_cursor
            ]

            update_lst = []
            used_materials = []

            # Duyệt qua các token được đề cập nhiều
            for token in top_mentioned_tokens:
                if len(update_lst) >= 5:
                    break

                if token not in posted_token_lst:  # Chưa được post
                    if token not in top_50_token_change_cursor:
                        # Xử lý token không nằm trong top 50 thay đổi
                        pipeline = [
                            {
                                "$match": {
                                    "symbol": token,
                                }
                            },
                            {"$sort": {"marketCapSevendayChangeScore": -1}},
                            {"$limit": 1},
                        ]

                        top_token = self.cdp._db["entity_change_ranking"].aggregate(
                            pipeline
                        )
                        for token_info in top_token:
                            token_tweets = self.cdp.get_top_tweets_by_token(token=token)
                            top_token_summarization = AnalysisPostingService(
                                symbol=token, token_info=token_info
                            )
                            response, replies, timeseries_data = (
                                top_token_summarization.get_summarized_top_token_post(
                                    tweets=token_tweets
                                )
                            )
                            print(f"token posted: {token}")

                            _id = token + "_" + str(round_timestamp(time.time()))
                            update_lst.append(
                                self._get_update_info(
                                    _id,
                                    token,
                                    response,
                                    replies,
                                    chain_id=token_info.get("chainId"),
                                )
                            )
                            used_materials.append(
                                self._used_materials(
                                    _id,
                                    tweets=token_tweets,
                                    timeseries_data=timeseries_data,
                                )
                            )

                    else:
                        pipeline = [
                            {
                                "$match": {
                                    "symbol": token,
                                }
                            },
                            {"$sort": {"marketCapSevendayChangeScore": -1}},
                            {"$limit": 1},
                        ]

                        top_token = self.cdp._db["entity_change_ranking"].aggregate(
                            pipeline
                        )

                        for token_info in top_token:
                            analysis = AnalysisPostingService(token_info=token_info)

                            response, replies, timeseries_data = (
                                analysis.get_period_analysis_post()
                            )

                            print(f"token posted: {token_info['symbol']}")

                            _id = (
                                token_info["symbol"]
                                + "_"
                                + str(round_timestamp(time.time()))
                            )
                            update_lst.append(
                                self._get_update_info(
                                    _id,
                                    token_info["symbol"],
                                    response,
                                    replies,
                                    chain_id=token_info.get("chainId"),
                                )
                            )

                            used_materials.append(
                                self._used_materials(
                                    _id,
                                    timeseries_data=timeseries_data,
                                )
                            )

            print(f"num period posts: {len(update_lst)}")
            self.cdp.update_docs(
                collection_name="agent_contents", data=update_lst, merge=True
            )
            self.cdp.update_docs(
                collection_name="agent_used_documents",
                data=used_materials,
                merge=True,
            )

        else:
            ## if chain is specific chain, then prioritize the daily most mentioned tokens
            print(chain_id)
            update_lst = []
            used_materials = []

            for token in top_mentioned_tokens:
                if len(update_lst) >= 5:
                    break
                else:
                    if token not in posted_token_lst:
                        if token not in ["btc", "bitcoin", "bb"]:
                            token_tweets = self.cdp.get_top_tweets_by_token(token=token)
                            top_token_summarization = AnalysisPostingService(
                                symbol=token
                            )
                            response, replies = (
                                top_token_summarization.get_keyword_summarized_tweet_post(
                                    tweets=token_tweets
                                )
                            )

                            _id = token + "_" + str(round_timestamp(time.time()))
                            update_lst.append(
                                self._get_update_info(
                                    _id, token, response, replies, chain_id=chain_id
                                )
                            )
                            used_materials.append(
                                self._used_materials(_id, tweets=token_tweets)
                            )

            self.cdp.update_docs(
                collection_name="agent_contents",
                data=update_lst,
                merge=True,
            )

            self.cdp.update_docs(
                collection_name="agent_used_documents",
                data=used_materials,
                merge=True,
            )

    def get_recent_category_analysis_post(
        self, posted_token_lst, top_category_mentioned_tokens
    ):
        pipeline = [
            {
                "$match": {
                    "updateTime": {"$gte": round_timestamp(time.time() - 86400)},
                    "categories": {"$in": [self.category]},
                    "marketCap": {"$gte": 100000},
                }
            },
            {"$sort": {"recentChangeScore": -1}},
        ]

        top_token = self.cdp._db["entity_change_ranking"].aggregate(pipeline)

        update_lst = []
        used_materials = []
        for token_info in top_token:
            _id = token_info["symbol"] + "_" + str(round_timestamp(time.time()))
            if len(update_lst) >= 2:
                break

            # If exists category mentioned token

            if len(top_category_mentioned_tokens) > 0:
                # If there is no new mentioned token, prioritize token have most change
                if len(
                    set(posted_token_lst).intersection(top_category_mentioned_tokens)
                ) == len(top_category_mentioned_tokens):
                    if token_info["symbol"] not in posted_token_lst:
                        analysis = AnalysisPostingService(token_info=token_info)
                        response, timeseries_data = analysis.get_recent_analysis_post()
                        if response != {}:
                            update_lst.append(
                                self._get_update_info(
                                    _id=_id,
                                    symbol=token_info["symbol"],
                                    response=response,
                                    chain_id=token_info.get("chainId"),
                                ),
                            )

                            used_materials.append(
                                self._used_materials(
                                    _id=_id, timeseries_data=timeseries_data
                                )
                            )

                # If yes, there is new mentioned token, prioritize token have most change and mention
                else:
                    if (token_info["symbol"] not in posted_token_lst) & (
                        token_info["symbol"] in top_category_mentioned_tokens
                    ):
                        analysis = AnalysisPostingService(token_info=token_info)
                        response = analysis.get_recent_analysis_post()
                        if response != {}:
                            update_lst.append(
                                self._get_update_info(
                                    _id=_id,
                                    symbol=token_info["symbol"],
                                    response=response,
                                    chain_id=token_info.get("chainId"),
                                ),
                            )

                            used_materials.append(
                                self._used_materials(
                                    _id=_id, timeseries_data=timeseries_data
                                )
                            )
            # If not exists category mentioned token, prioritize token have most change

            else:
                if token_info["symbol"] not in posted_token_lst:
                    analysis = AnalysisPostingService(token_info=token_info)
                    response = analysis.get_recent_analysis_post()
                    if response != {}:
                        update_lst.append(
                            self._get_update_info(
                                _id=_id,
                                symbol=token_info["symbol"],
                                response=response,
                                chain_id=token_info.get("chainId"),
                            ),
                        )

                        used_materials.append(
                            self._used_materials(
                                _id=_id, timeseries_data=timeseries_data
                            )
                        )

        print(f"num recent posts: {len(update_lst)}")

        self.cdp.update_docs(
            collection_name=f"agent_{self.category.lower()}_contents",
            data=update_lst,
            merge=True,
        )

        self.cdp.update_docs(
            collection_name="agent_used_documents",
            data=used_materials,
            merge=True,
        )

    def get_period_category_analysis_post(
        self, posted_token_lst, top_category_mentioned_tokens
    ):
        print(f"posted tokens: {posted_token_lst}")
        top_50_token_pipeline = [
            {
                "$match": {
                    "updateTime": {"$gte": round_timestamp(time.time() - 86400)},
                    "categories": {"$in": [self.category]},
                    "marketCap": {"$gte": 100000},
                }
            },
            {"$sort": {"marketCapSevendayChangeScore": -1}},
            {"$limit": 50},
        ]

        top_50_token_cursor = self.cdp._db["entity_change_ranking"].aggregate(
            top_50_token_pipeline
        )

        top_50_token_change_cursor = [token["symbol"] for token in top_50_token_cursor]

        update_lst = []
        used_materials = []
        # If yes, there is new mentioned token, prioritize token have most change and mention
        if len(
            set(posted_token_lst).intersection(top_category_mentioned_tokens)
        ) != len(top_category_mentioned_tokens):
            for token in top_category_mentioned_tokens:
                _id = token + "_" + str(round_timestamp(time.time()))
                if len(update_lst) >= 2:
                    break
                if token not in posted_token_lst:
                    if token not in top_50_token_change_cursor:
                        pipeline = [
                            {
                                "$match": {
                                    "symbol": token,
                                }
                            },
                            {"$sort": {"marketCapSevendayChangeScore": -1}},
                            {"$limit": 1},
                        ]

                        top_token = self.cdp._db["entity_change_ranking"].aggregate(
                            pipeline
                        )
                        for token_info in top_token:
                            token_tweets = self.cdp.get_top_tweets_by_token(token=token)
                            top_token_summarization = AnalysisPostingService(
                                symbol=token, token_info=token_info
                            )
                            response, replies, timeseries_data = (
                                top_token_summarization.get_summarized_top_token_post(
                                    tweets=token_tweets
                                )
                            )
                            print(f"token posted: {token}")

                            update_lst.append(
                                self._get_update_info(
                                    _id,
                                    token,
                                    response,
                                    replies,
                                    chain_id=token_info.get("chainId"),
                                )
                            )
                            used_materials.append(
                                self._used_materials(
                                    _id=_id,
                                    tweets=token_tweets,
                                    timeseries_data=timeseries_data,
                                )
                            )

                    else:
                        pipeline = [
                            {
                                "$match": {
                                    "symbol": token,
                                }
                            },
                            {"$sort": {"marketCapSevendayChangeScore": -1}},
                            {"$limit": 1},
                        ]

                        top_token = self.cdp._db["entity_change_ranking"].aggregate(
                            pipeline
                        )

                        for token_info in top_token:
                            # if (token_info["symbol"] not in posted_token_lst) & (
                            #     token_info["symbol"] in top_mentioned_tokens
                            # ):
                            analysis = AnalysisPostingService(token_info=token_info)

                            response, replies, timeseries_data = (
                                analysis.get_period_analysis_post()
                            )

                            print(f"token posted: {token_info['symbol']}")

                            update_lst.append(
                                self._get_update_info(
                                    token_info["symbol"],
                                    response,
                                    replies,
                                    chain_id=token_info.get("chainId"),
                                )
                            )

                            used_materials.append(
                                self._used_materials(
                                    _id=_id,
                                    timeseries_data=timeseries_data,
                                )
                            )

            print(f"num period posts: {len(update_lst)}")
            self.cdp.update_docs(
                collection_name=f"agent_{self.category.lower()}_contents",
                data=update_lst,
                merge=True,
            )

            self.cdp.update_docs(
                collection_name="agent_used_documents",
                data=used_materials,
                merge=True,
            )

            isPosted = True

        # If not, not post
        else:
            isPosted = False

        return isPosted

    def get_keyword_summary_post(self):
        update_lst = []
        used_materials = []
        _id = self.keyword + "_" + str(round_timestamp(time.time()))
        keyword_tweets = self.cdp.get_top_tweets_by_keyword(keyword=self.keyword)
        top_token_summarization = AnalysisPostingService(symbol=self.keyword)
        response, replies = top_token_summarization.get_keyword_summarized_tweet_post(
            tweets=keyword_tweets
        )

        keyword_tag = f"{self.keyword}_{round_timestamp(timestamp=time.time(), round_time=self.interval)}"
        update_lst.append(
            self._get_update_info(
                _id=keyword_tag,
                symbol=self.keyword,
                response=response,
                replies=replies,
                type="keyword",
            )
        )
        used_materials.append(self._used_materials(_id=_id, tweets=keyword_tweets))

        self.cdp.update_docs(
            collection_name="agent_contents",
            data=update_lst,
            merge=True,
        )

        self.cdp.update_docs(
            collection_name="agent_used_documents",
            data=used_materials,
            merge=True,
        )

    def _get_update_info(
        self, _id, symbol, response, replies=[], type="", chain_id=None
    ):
        update_info = dict()
        if type == "":
            update_info["type"] = "analysis"
        else:
            update_info["type"] = type

        update_info["keyWord"] = symbol
        update_info["lastUpdated"] = round_timestamp(time.time())
        update_info["upToDate"] = True
        update_info["content"] = response
        update_info["_id"] = _id
        update_info["chainId"] = chain_id
        if len(replies) > 0:
            reply_dct = dict()
            reply_dct["replies"] = replies
            update_info["repliesContent"] = reply_dct

        return update_info

    def _used_materials(self, _id, tweets=[], timeseries_data=[]):
        used_materials = dict()
        used_materials["_id"] = _id
        used_materials["timestamp"] = round_timestamp(
            timestamp=time.time(), round_time=3600
        )
        if tweets != []:
            used_materials["tweets"] = tweets
        if timeseries_data != []:
            used_materials["timeseries"] = timeseries_data

        return used_materials

    def _get_top_mentioned_tokens(self, chain_id):
        if chain_id == "all":
            top_token_mentioned_pipeline = [
                {
                    "$match": {
                        "mentionTimes": {"$gte": 50},
                        "timestamp": {"$gte": time.time() - TimeConstants.A_DAY},
                    }
                },
                {"$sort": {"meanView": -1}},
            ]
        else:
            top_token_mentioned_pipeline = [
                {
                    "$match": {
                        "chainId": self.chain_id,
                        "tags": "symbol",
                        "meanView": {"$gte": 1000},
                        "timestamp": {"$gte": time.time() - TimeConstants.A_DAY},
                    }
                },
                {"$sort": {"mentionTimes": -1}},
            ]

        ranking = self.cdp._db["entity_mentioned_ranking"].aggregate(
            top_token_mentioned_pipeline
        )
        top_mentioned_tokens = [token["symbol"] for token in ranking]

        return top_mentioned_tokens

    def _get_top_category_mentioned_tokens(self):
        top_category_token_mentioned_pipeline = [
            {
                "$match": {
                    "mentionTimes": {"$gte": 5},
                    "tags": "symbol",
                    "timestamp": {"$gte": time.time() - TimeConstants.A_DAY},
                    "categories": {"$in": [self.category]},
                }
            },
            {"$sort": {"meanView": -1}},
        ]

        ranking_category = self.cdp._db["entity_mentioned_ranking"].aggregate(
            top_category_token_mentioned_pipeline
        )
        top_category_mentioned_tokens = [token["symbol"] for token in ranking_category]

        return top_category_mentioned_tokens

    def _get_posted_tokens(self, category=""):
        if category == "":
            posted_info = (
                MongoDBCDP()
                ._db["agent_contents"]
                .find(
                    {
                        "lastUpdated": {
                            "$gte": round_timestamp(time.time() - TimeConstants.DAYS_3)
                        },
                        "isPosted": {"$exists": True},
                    }
                )
            )
        else:
            posted_info = (
                MongoDBCDP()
                ._db["agent_{self.category.lower()}_contents"]
                .find(
                    {
                        "lastUpdated": {
                            "$gte": round_timestamp(time.time() - TimeConstants.DAYS_30)
                        },
                        "isPosted": {"$exists": True},
                    }
                )
            )
        posted_token_lst = []
        for token in posted_info:
            if "keyWord" in token:
                posted_token_lst.append(token["keyWord"].lower())

        posted_token_lst = list(set(posted_token_lst))

        return posted_token_lst
