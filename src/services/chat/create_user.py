import os
import sys
import time

import requests

sys.path.append(os.getcwd())

from dotenv import load_dotenv

from src.utils.logger import get_logger
from src.databases.mongodb_community import MongoDBCommunity

load_dotenv()
logger = get_logger("Create User")


class CreateUser:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.mongodb = MongoDBCommunity()
        self.api_chat = os.getenv("API_CHATBOT")
        self.authorize_chat = os.getenv("AUTHORIZE_CHATBOT")

    def create_user(self):
        try:
            headers = {
                "accept": "application/json",
                "X-API-KEY": self.authorize_chat,
                "Content-Type": "application/json",
            }
            request_body = {
                "user_id": self.user_id,
            }
            response = requests.post(
                f"{self.api_chat}/users",
                headers=headers,
                json=request_body,
            )
            if response.json().get("message") == "Created successfully!":
                data = {
                    "_id": self.user_id,
                    "createdAt": int(time.time()),
                }

                self.mongodb.update_docs(
                    collection_name="chat_users",
                    data=[data],
                )
                return {"status": "success"}
            else:
                return {"status": "error"}
            
        except Exception as e:
            logger.error(f"Error in create_user: {str(e)}")
            return
            

