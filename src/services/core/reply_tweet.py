import os

import requests
from dotenv import load_dotenv

from src.constants.llm.agent_prompt import TuningReplyPromptTemplate
from src.services.llm.communication import LLMCommunication

load_dotenv()


class ReplyTweetService:
    def __init__(self):
        self.api_chat = os.getenv("API_CHAT")
        self.x_api_key = os.getenv("X_API_KEY")
        self.llm = LLMCommunication()

    def reply_tweet(self, original_tweet, user_reply):
        request_data = {
            "text": user_reply,
            "num_keywords": 10,
            "num_context": 0,
            "use_for_twitter": True,
            "user_id": "twitter",
            "is_from": "web",
            "max_character": 270,
            "context": original_tweet,
        }
        response = requests.post(
            self.api_chat,
            headers={
                "X-API-KEY": self.x_api_key,
            },
            json=request_data,
        )
        result = response.json().get("answer")

        return result

    def post_process(self, original_tweet, user_reply):
        reply_tweet = self.reply_tweet(original_tweet, user_reply)
        tuning_reply_prompt_template = TuningReplyPromptTemplate()
        template = tuning_reply_prompt_template.create_template()
        prompt = template.format(
            original_tweet=original_tweet,
            reply_tweet=user_reply,
            initial_reply_tweet=reply_tweet,
        )

        response = self.llm.send_prompt(prompt)
        return response
