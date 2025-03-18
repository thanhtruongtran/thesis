from sanic import Blueprint, json
from sanic_ext import openapi

from src.services.feed.get_feed import GetFeed
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
