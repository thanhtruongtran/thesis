import random

from sanic import Blueprint, json
from sanic_ext import openapi

from src.services.feed.get_analytics import GetAnalytics
from src.services.feed.get_feed import GetFeed
from src.services.feed.interest_feed import GetInterestFeed
from src.services.feed.user_preferences import UserPreferencesService
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
    feeds = GetInterestFeed(user_id).get_news_preferences()

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


@bp.get("/feed/analytics")
@openapi.tag("Feed")
@openapi.summary("Get Analytics Feed")
async def get_analytics_feed(request):
    analytics = GetAnalytics().get_analytics()
    result = []
    for doc in analytics:
        result.append(
            {
                "img_url": doc["imgUrl"],
                "analysis": doc["analysis"],
                "tag": doc["tag"],
                "title": "7D Price" if doc["tag"] == "token" else "7D TVL",
                "timeseries_data": doc["timeseries_data"].get("price", [])
                or doc["timeseries_data"].get("tvl", []),
                "website": doc["website"],
            }
        )
    random.shuffle(result)

    return json(result)


@bp.get("/user-preferences")
@openapi.tag("User Preferences")
@openapi.summary("Get User Preferences")
@openapi.parameter("userId", str, "query", required=True)
@openapi.secured("Authorization")
async def get_user_preferences(request):
    try:
        user_id = request.args.get("userId")
        if not user_id:
            return json({"error": "userId is required"}, status=400)

        preferences = UserPreferencesService().get_preferences(user_id)
        return json(preferences)
    except Exception as e:
        logger.error(f"Error getting user preferences: {str(e)}")
        return json({"error": "Internal server error"}, status=500)


@bp.post("/user-preferences")
@openapi.tag("User Preferences")
@openapi.summary("Save User Preferences")
@openapi.secured("Authorization")
async def save_user_preferences(request):
    try:
        data = request.json
        if not data or "userId" not in data or "preferences" not in data:
            return json({"error": "userId and preferences are required"}, status=400)

        user_id = data["userId"]
        preferences = data["preferences"]

        # Validate required fields
        required_fields = ["defiSector", "assetTypes", "topics", "completedOnboarding"]
        for field in required_fields:
            if field not in preferences:
                return json({"error": f"Missing required field: {field}"}, status=400)

        UserPreferencesService().save_preferences(user_id, preferences)
        return json({"message": "Preferences saved successfully"})
    except Exception as e:
        logger.error(f"Error saving user preferences: {str(e)}")
        return json({"error": "Internal server error"}, status=500)


@bp.post("/news-history")
@openapi.tag("News History")
@openapi.summary("Save News View History")
@openapi.secured("Authorization")
async def save_news_view_history(request):
    try:
        data = request.json
        user_id = data.get("userId")
        news_id = data.get("newsId")
        entities = data.get("entities")
        timestamp = data.get("timestamp")
        if not user_id or not news_id or not timestamp:
            return json(
                {"error": "userId, newsId, and timestamp are required"}, status=400
            )

        from src.databases.mongodb_community import MongoDBCommunity

        db = MongoDBCommunity()
        success = db.save_news_view_history(user_id, news_id, timestamp, entities)
        if success:
            return json({"message": "News view history saved successfully"})
        else:
            return json({"error": "Failed to save news view history"}, status=500)
    except Exception as e:
        logger.error(f"Error saving news view history: {str(e)}")
        return json({"error": "Internal server error"}, status=500)
