import json
import os
from typing import List, Union

from dotenv import load_dotenv
from openai import AzureOpenAI

from src.constants.llm.llm_config import AzureOpenAIModel

load_dotenv()
MAX_LENGTH = 16384
SYSTEM_PROMPT = "You are ai influencer in Web3 Community"


class LLMCommunication:
    def __init__(self, model_name: str = None):
        self.gpt_model = (
            model_name if model_name else os.getenv("LARGE_LANGUAGE_MODEL_OPENAI")
        )
        config = AzureOpenAIModel()
        self.client = config.make_client()

    def send_prompt(self, prompt: str) -> list:
        try:
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt[:MAX_LENGTH]},
                ],
                model=self.gpt_model,
                temperature=0.75,
                max_tokens=256,
                top_p=1,
            )
            response = chat_completion.choices[0].message.content
            return response
        except Exception as e:
            print(e)
            return []

    def embedding(self, text: str) -> List[Union[float, int]]:
        try:
            client = AzureOpenAI(
                api_key=os.getenv("EMBEDDING_OPENAI_API"),
                api_version=os.getenv("EMBEDDING_API_VERSION"),
                azure_endpoint=os.getenv("EMBEDDING_AZURE_ENDPOINT"),
            )

            response = client.embeddings.create(
                model="text-embedding-3-small",
                input=text,
            )
            embedding = response.data[0].embedding

            return embedding
        except Exception as e:
            print(e)
            return []

    def post_process(self, response: str) -> list:
        """
        Chuyển chuỗi JSON response từ LLM thành danh sách chuỗi văn bản.

        Args:
            response (str): Chuỗi JSON từ API LLM.

        Returns:
            list: Danh sách các chuỗi văn bản.
        """
        try:
            # Chuyển chuỗi JSON thành danh sách Python
            processed_response = json.loads(response)

            # Kiểm tra kết quả
            if isinstance(processed_response, list) and all(
                isinstance(item, str) for item in processed_response
            ):
                return processed_response
            else:
                raise ValueError("Response is not a list of strings.")

        except json.JSONDecodeError as e:
            print(f"JSONDecodeError: {e}")
            return []
        except ValueError as e:
            print(f"ValueError: {e}")
            return []
