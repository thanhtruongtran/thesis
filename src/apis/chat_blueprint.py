from sanic import Blueprint, json, Request
from sanic_ext import openapi

# from src.decorators.auth import authenticate
from src.services.chat.response import ChatResponse
from src.services.chat.create_user import CreateUser
from src.utils.logger import get_logger


logger = get_logger("Chat Blueprint")
bp = Blueprint("chat_blueprint", url_prefix="/")


@bp.get("/response")
@openapi.tag("Chatbot")
@openapi.summary("Chatbot Response")
@openapi.parameter("query", str, "query", required=True)
@openapi.parameter("id", str, "query", required=True)
@openapi.secured("Authorization")
# @authenticate()
async def chat_response(request: Request):
    query = request.args.get("query")
    user_id = request.args.get("id")
    response = ChatResponse(user_id).get_response(query)
    return json(response)


@bp.post("/create_user")
@openapi.tag("Chatbot")
@openapi.summary("Create User")
@openapi.parameter("id", str, "query", required=True)
@openapi.secured("Authorization")
# @authenticate()
async def create_user(request: Request):
    user_id = request.args.get("id")
    response = CreateUser(user_id=user_id).create_user()
    return json(response)
