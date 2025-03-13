import os
import sys

sys.path.append(os.getcwd())

import time

from flasgger import Swagger
from flask import Flask, jsonify, request
from flask_restful import Api

from src.constants.time import TimeConstants
from src.databases.mongodb_cdp import MongoDBCDP
from src.services.core.analysis_posting import AnalysisPostingService
from src.services.core.news_posting import NewsPostingService
from src.services.core.reply_tweet import ReplyTweetService
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

app = Flask(__name__)
api = Api(app)
swagger = Swagger(app)

logger = get_logger("ReplyTweetAPI")


@app.route("/reply_tweet", methods=["POST"])
def reply_tweet():
    data = request.get_json()
    original_tweet = data.get("original_tweet")
    user_reply = data.get("user_reply")
    service = ReplyTweetService()
    response = service.post_process(original_tweet, user_reply)

    return jsonify({"response": response})


@app.route("/get_post", methods=["POST"])
def get_post():
    data = request.get_json()
    idCoingecko = data.get("idCoingecko")
    content = data.get("content")
    type = data.get("type")

    if type == "analysis":
        if idCoingecko is not None:
            token_info = (
                MongoDBCDP()
                ._db["entity_change_ranking"]
                .find_one({"idCoingecko": idCoingecko})
            )
            if token_info is not None:
                service = AnalysisPostingService(token_info=token_info)

                response = service.get_request_analysis_post(isContent=content)

            else:
                response = {}

            return jsonify({"response": response})

        else:
            posted_info = (
                MongoDBCDP()
                ._db["agent_contents"]
                .find(
                    {
                        "lastUpdated": {
                            "$gte": round_timestamp(time.time() - TimeConstants.DAYS_3)
                        },
                    }
                )
            )
            token_lst = []
            for token in posted_info:
                if "keyWord" in token:
                    token_lst.append(token["keyWord"])

            pipeline = [
                {
                    "$match": {
                        "updateTime": {"$gte": round_timestamp(time.time())},
                        "marketCap": {"$gte": 5000000},
                    }
                },
                {"$sort": {"recentChangeScore": -1}},
                {"$limit": 20},
            ]

            top_token = MongoDBCDP()._db["entity_change_ranking"].aggregate(pipeline)

            update_lst = []
            for token_info in top_token:
                if len(update_lst) >= 4:
                    break

                if token_info["symbol"] not in token_lst:
                    analysis = AnalysisPostingService(token_info=token_info)

                    response = analysis.get_recent_analysis_post(isContent=content)
                    if response != {}:
                        response["idCoingecko"] = token_info["idCoingecko"]
                        response["symbol"] = token_info["symbol"]
                        update_lst.append(response)

            return jsonify({"response": update_lst})

    elif type == "news":
        service = NewsPostingService()
        response = service.post_news_daily(isContent=content)

        return jsonify({"response": response})

    else:
        return jsonify({"response": "No such that type"})


if __name__ == "__main__":
    port = int(os.environ.get("AGENT_REPLY_TWEET_PORT", 1204))
    app.run(host="0.0.0.0", port=port, debug=True)
