from sanic import Blueprint, json, Request
from sanic_ext import openapi

# from src.decorators.auth import authenticate
from src.services.chat.response import ChatResponse
from src.databases.mongodb_community import MongoDBCommunity
# from src.services.chat.create_user import CreateUser
from src.utils.logger import get_logger


logger = get_logger("Chat Blueprint")
mongodb = MongoDBCommunity()
bp = Blueprint("chat_blueprint", url_prefix="/")


@bp.get("/response")
@openapi.tag("Chatbot")
@openapi.summary("Chatbot Response")
@openapi.parameter("query", str, "query", required=True)
# @openapi.parameter("id", str, "query", required=True)
@openapi.secured("Authorization")
# @authenticate()
async def chat_response(request: Request):
    query = request.args.get("query")
    # user_id = request.args.get("id")
    response = ChatResponse().get_response(query)
    return json(response)


# @bp.post("/create_user")
# @openapi.tag("Chatbot")
# @openapi.summary("Create User")
# @openapi.parameter("id", str, "query", required=True)
# @openapi.secured("Authorization")
# # @authenticate()
# async def create_user(request: Request):
#     user_id = request.args.get("id")
#     response = CreateUser(user_id=user_id).create_user()
#     return json(response)


@bp.post("/save-interest")
@openapi.tag("Chatbot")
@openapi.summary("Save Interest")
@openapi.parameter("id", str, "query", required=True)
@openapi.secured("Authorization")
async def save_interest(request: Request):
    user_id = request.args.get("id")
    request_json = request.json 

    interests = request_json.get("interest", [])

    if not isinstance(interests, list):
        return json({"status": "error", "message": "Invalid data format. 'interest' must be a list."}, status=400)

    data = {
        "_id": user_id,
        "interest": {
            "CHAIN": interests[0],
            "TREND": interests[1],
            "TOKEN": interests[2],
        },
        "isOnboarded": True
    }

    mongodb.update_docs(
        collection_name="chat_query",
        data=[data],
    )

    return json({"status": "success"})


@bp.get("/user-info")
@openapi.tag("Chatbot")
@openapi.summary("Get User Info")
@openapi.parameter("id", str, "query", required=True)
@openapi.secured("Authorization")
async def get_user_info(request: Request):
    user_id = request.args.get("id")
    user_data = mongodb._db["chat_query"].find(
        {"_id": user_id},
    )
    
    user_data = list(user_data)
    data = {
        "_id": user_id,
        "interest": user_data[0].get("interest", []),
        "isOnboarded": user_data[0].get("isOnboarded", False)
    }

    return json(data)

