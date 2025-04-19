from sanic import Blueprint, json
from sanic_ext import openapi

from src.services.feed.get_feed import GetFeed
from src.services.feed.interest_feed import GetInterestFeed
from src.utils.logger import get_logger

logger = get_logger("Feed Blueprint")
bp = Blueprint("feed_blueprint", url_prefix="/")


@bp.get("/feed")
@openapi.tag("Feed")
@openapi.summary("Get Feed")
@openapi.parameter("user_id", str, "query", required=True)
@openapi.secured("Authorization")
async def get_feed(request):
    user_id = request.args.get("user_id")
    feeds = GetFeed(user_id).get_feed()

    return json(feeds)


@bp.get("/feed/interest")
@openapi.tag("Feed")
@openapi.summary("Get Interest Feed")
@openapi.parameter("user_id", str, "query", required=True)
@openapi.secured("Authorization")
async def get_interest_feed(request):
    user_id = request.args.get("user_id")
    feeds = GetInterestFeed(user_id).get_interest_feed()

    return json(feeds)


@bp.get("/feed/all")
@openapi.tag("Feed")
@openapi.summary("Get All Feed")
@openapi.parameter("user_id", str, "query", required=True)
@openapi.secured("Authorization")
async def get_all_feed(request):
    user_id = request.args.get("user_id")
    feed_v0 = GetFeed(user_id).get_feed()
    feed_v1 = GetInterestFeed(user_id).get_interest_feed()

    all_feed = feed_v0 + feed_v1
    return json(all_feed)