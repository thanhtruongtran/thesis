import json
import os
import re
import sys
import time

import requests

sys.path.append(os.getcwd())

from dotenv import load_dotenv

from src.constants.llm.agent_prompt import EntityExtractionPromptTemplate
from src.databases.mongodb_community import MongoDBCommunity
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger("Chat Response")


class ChatResponse:
    def __init__(
        self, user_id='16531087-1950-47d1-8f53-d44ec5dcd6b3', session_id="", from_platform='other', use_for_twitter=False
    ):
        self.user_id = user_id
        self.session_id = session_id
        self.from_platform = from_platform
        self.use_for_twitter = use_for_twitter
        self.api_chat = os.getenv("API_CHATBOT")
        self.authorize_chat = os.getenv("AUTHORIZE_CHATBOT")
        self.FROM_CHATBOT = os.getenv("FROM_CHATBOT")
        self.CLIENT_ID = os.getenv("CLIENT_ID")
        self.mongodb = MongoDBCommunity()
        self.llm = LLMCommunication()

    def get_response(
        self, text, num_keywords=10, num_context=0, context=""
    ):
        try:
            headers = {
                "X-API-KEY": self.authorize_chat,
                "Content-Type": "application/json",
            }
            request_body = {
                "text": text,
                "num_keywords": num_keywords,
                "num_context": num_context,
                "context": context,
            }
            response = requests.post(
                f"{self.api_chat}/chat?user_id={self.user_id}&from_chatbot={self.FROM_CHATBOT}&client_id={self.CLIENT_ID}&from_platform={self.from_platform}&use_for_twitter={self.use_for_twitter}",
                headers=headers,
                json=request_body,
            )
            response = response.json().get("answer")
            self.save_query(text, response)

            return response
        except Exception as e:
            logger.error(f"Error in get_response: {str(e)}")
            return None

    def extract_keywords(self, text):
        pass

    def extract_entities(self, query):
        ner_prompt_template = EntityExtractionPromptTemplate()
        template = ner_prompt_template.create_template()
        prompt = template.format(text=query)

        response = self.llm.send_prompt(prompt)
        return response

    def format_entities(self, entities):
        json_pattern = r"```json\s*(.*?)\s*```"
        fallback_pattern = r"(\{[\s\S]*\})"
        json_match = re.search(json_pattern, entities, re.DOTALL)
        fallback_match = re.search(fallback_pattern, entities, re.DOTALL)

        if json_match:
            json_str = json_match.group(1)
        if fallback_match:
            json_str = fallback_match.group(1)

        try:
            result_dict = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Error in format_entities: {str(e)}")
            result_dict = {}

        return result_dict

    # function to save query of user, extract keywords and entities, and save in database
    def save_query(self, query, response):
        timestamp = int(time.time())
        # entities = self.extract_entities(query)
        # entities = self.format_entities(entities)
        entities = {}

        # # if all values are empty, then return
        # for key, value in entities.items():
        #     if value:
        #         break
        # else:
        #     logger.info("No entities found")
        #     return

        data = {
            "_id": self.user_id,
            "history": {
                timestamp: {
                    "query": query,
                    "response": response,
                    "entities": entities,
                }
            },
        }

        self.mongodb.update_docs(
            collection_name="chat_query",
            data=[data],
        )
        logger.info("Query saved successfully")
        return data


if __name__ == "__main__":
    chat_response = ChatResponse()
    response = chat_response.get_response(
        text="What is DEX",
    )
    print(response)
