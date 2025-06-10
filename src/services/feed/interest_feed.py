import os
import sys
import time

import numpy as np

# import torch

sys.path.append(os.getcwd())

from sentence_transformers import SentenceTransformer

# from sentence_transformers import util
from src.databases.mongodb_cdp import MongoDBCDP
from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger

logger = get_logger("Get Interest Feed")


class GetInterestFeed:
    def __init__(self, user_id=None):
        self.user_id = user_id
        self.mongodb_community = MongoDBCommunity()
        self.mongodb_cdp = MongoDBCDP()
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def get_user_preferences(self):
        cursor = self.mongodb_community._db["user_preferences"].find(
            {"userId": self.user_id}
        )
        cursor = list(cursor)
        preferences = cursor[0]["preferences"]
        embedding = cursor[0]["embedding"]

        return preferences, embedding

    def get_news_preferences(self):
        preferences, embedding = self.get_user_preferences()

        cursor = (
            self.mongodb_community._db["news_articles"]
            .find(
                {
                    "publish_date_timestamp": {"$gte": int(time.time()) - 86400 * 3},
                    # "embedding": {"$exists": True},
                    "entities": {"$exists": True},
                    "link_entities": {"$exists": True},
                },
                {
                    "_id": 1,
                    "title": 1,
                    "publish_date_timestamp": 1,
                    "text": 1,
                    "summary": 1,
                    "img_url": 1,
                    "url": 1,
                    "type": 1,
                    "entities": 1,
                    "embedding": 1,
                    "link_entities": 1,
                },
            )
            .sort("publish_date_timestamp", -1)
        )

        related_news = []
        for news in cursor:
            related_news.append(
                {
                    "_id": news.get("_id"),
                    "keyWord": news.get("title", ""),
                    "lastUpdated": news.get("publish_date_timestamp", ""),
                    "content": news.get("summary", ""),
                    "type": "news",
                    "imgUrl": news.get("img_url", ""),
                    "url": news.get("url", ""),
                    "entities": news.get("entities", []),
                    "link_entities": news.get("link_entities"),
                }
            )
            # news_embedding = torch.tensor(news.get("embedding"))
            # user_embedding = torch.tensor(embedding)

            # device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            # news_embedding = news_embedding.to(device)
            # user_embedding = user_embedding.to(device)
            # cosine_similarity = util.pytorch_cos_sim(user_embedding, news_embedding)
            # if cosine_similarity > 0:
            #     related_news.append(
            #         {
            #             "keyWord": news.get("title", ""),
            #             "lastUpdated": news.get("publish_date_timestamp", ""),
            #             "content": news.get("summary", ""),
            #             "type": "news",
            #             "imgUrl": news.get("img_url", ""),
            #             "url": news.get("url", ""),
            #             "entities": news.get("entities", []),
            #             "link_entities": news.get("link_entities")
            #         }
            #     )
        return related_news

    def get_interest_feed(self):
        timestamp = int(time.time()) - 86400 * 3
        feed = (
            self.mongodb_community._db["news_articles"]
            .find(
                {
                    "publish_date_timestamp": {"$gte": timestamp},
                    "entities": {"$exists": True},
                    "link_entities": {"$exists": True},
                },
                {
                    "title": 1,
                    "publish_date_timestamp": 1,
                    "text": 1,
                    "summary": 1,
                    "img_url": 1,
                    "url": 1,
                    "type": 1,
                    "entities": 1,
                    "link_entities": 1,
                },
            )
            .sort("publish_date_timestamp", -1)
        )
        result = [
            {
                "keyWord": i["title"],
                "lastUpdated": i["publish_date_timestamp"],
                "content": i["summary"],
                "type": "news",
                "imgUrl": i["img_url"],
                "url": i["url"],
                "entities": i["entities"],
                "link_entities": i["link_entities"],
            }
            for i in feed
        ]

        return result

    def analyze_entity_from_news_chat_history(self):
        timestamp = int(time.time()) - 86400 * 3
        cursor_1 = self.mongodb_community._db["news_view_history"].find(
            {
                "timestamp": {"$gte": timestamp},
                "userId": self.user_id,
                "entities": {"$exists": True},
            },
            {"entities": 1},
        )

        cursor_2 = self.mongodb_community._db["chat_query"].find(
            {
                "_id": self.user_id,
            },
        )
        chat_doc = list(cursor_2)
        if not chat_doc:
            print(f"No chat history found for user {self.user_id}")
            return []
        chat_doc = chat_doc[0]

        unique_entities_with_types = set()

        for doc in cursor_1:
            if "entities" in doc and isinstance(doc["entities"], dict):
                for entity, entity_type in doc["entities"].items():
                    if (
                        isinstance(entity, str)
                        and entity
                        and isinstance(entity_type, str)
                        and entity_type
                    ):
                        unique_entities_with_types.add((entity, entity_type))

        if "history" in chat_doc and isinstance(chat_doc["history"], dict):
            for time_stamp_str, value in chat_doc["history"].items():
                try:
                    time_stamp = int(time_stamp_str)
                    if time_stamp < timestamp:
                        continue
                    if "entities" in value and isinstance(value["entities"], dict):
                        for entity, entity_type in value["entities"].items():
                            if (
                                isinstance(entity, str)
                                and entity
                                and isinstance(entity_type, str)
                                and entity_type
                            ):
                                unique_entities_with_types.add((entity, entity_type))
                except ValueError:
                    print(
                        f"Skipping invalid timestamp key in chat history: {time_stamp_str}"
                    )
                    continue

        return list(unique_entities_with_types)

    def create_interest_prompt_and_embedding(self, entities_with_types: list) -> tuple:
        """
        Creates a text prompt summarizing user interests based on entities and types,
        and generates an embedding for the prompt.

        Args:
            entities_with_types: A list of (entity, type) tuples.

        Returns:
            A tuple containing the generated prompt string and its embedding.
            Returns (None, None) if no entities are provided.
        """
        if not entities_with_types:
            print("No entities with types provided to create prompt and embedding.")
            return None, None

        interest_phrases = [
            f"{entity} ({entity_type})" for entity, entity_type in entities_with_types
        ]
        prompt = "User is interested in: " + ", ".join(interest_phrases)

        print(f"Generated interest prompt: {prompt}")

        try:
            embedding = self.model.encode(prompt)
            print("Successfully generated embedding for the interest prompt.")
            return prompt, embedding.tolist()
        except Exception as e:
            print(f"Error generating embedding for prompt: {e}")
            return prompt, None

    def find_similar_news_by_embedding(
        self,
        user_interest_embedding: list,
        time_window_days: int = 3,
        limit: int | None = None,
    ):
        """
        Finds news articles from the last 'time_window_days' with embeddings similar to the user interest embedding.

        Args:
            user_interest_embedding: The embedding vector of the user's interest profile.
            time_window_days: The number of past days to search for news.
            limit: The maximum number of articles to return. If None, return all found articles.

        Returns:
            A list of news article documents, sorted by similarity score (if possible),
            formatted similarly to the output of get_news_preferences.
        """
        if user_interest_embedding is None:
            return []

        timestamp = int(time.time()) - 86400 * time_window_days

        query = {
            "publish_date_timestamp": {"$gte": timestamp},
            "embedding": {"$exists": True},
            "entities": {"$exists": True},
            "link_entities": {"$exists": True},
        }

        projection = {
            "_id": 1,
            "title": 1,
            "publish_date_timestamp": 1,
            "text": 1,
            "summary": 1,
            "img_url": 1,
            "url": 1,
            "type": 1,
            "entities": 1,
            "embedding": 1,
            "link_entities": 1,
        }

        cursor = self.mongodb_community._db["news_articles"].find(query, projection)

        suggested_news_with_similarity = []
        user_embedding_np = np.array(user_interest_embedding)

        for article in cursor:
            if "embedding" in article and isinstance(article["embedding"], list):
                try:
                    article_embedding_np = np.array(article["embedding"])
                    if user_embedding_np.shape == article_embedding_np.shape:
                        similarity = np.dot(user_embedding_np, article_embedding_np) / (
                            np.linalg.norm(user_embedding_np)
                            * np.linalg.norm(article_embedding_np)
                        )
                        suggested_news_with_similarity.append(
                            {"article": article, "similarity": similarity}
                        )
                except Exception:
                    pass

        suggested_news_with_similarity.sort(key=lambda x: x["similarity"], reverse=True)

        if limit is not None:
            suggested_news_with_similarity = suggested_news_with_similarity[:limit]

        formatted_news = []
        for item in suggested_news_with_similarity:
            article = item["article"]
            formatted_news.append(
                {
                    "_id": article.get("_id"),
                    "keyWord": article.get("title", ""),
                    "lastUpdated": article.get("publish_date_timestamp", ""),
                    "content": article.get("summary", ""),
                    "type": article.get("type", "news"),
                    "imgUrl": article.get("img_url", ""),
                    "url": article.get("url", ""),
                    "entities": article.get("entities", []),
                    "link_entities": article.get("link_entities"),
                }
            )

        return formatted_news

    def evaluate_embedding_similarity(
        self,
        user_id: str,
        cutoff_timestamp: int,
        history_time_window_days: int = 3,
        evaluation_time_window_days: int = 3,
    ) -> float:
        """
        Evaluates the average embedding similarity between the user's interest profile
        and suggested news articles published after the cutoff timestamp.

        Args:
            user_id: The ID of the user to evaluate.
            cutoff_timestamp: The timestamp to split historical data.
            history_time_window_days: Days of history before cutoff to use for interest profiling.
            evaluation_time_window_days: Days after cutoff to look for suggested news.

        Returns:
            The average cosine similarity score, or 0.0 if no articles are found or embedding fails.
        """

        original_user_id = self.user_id
        self.user_id = user_id

        history_start_timestamp = cutoff_timestamp - 86400 * history_time_window_days

        cursor_1 = self.mongodb_community._db["news_view_history"].find(
            {
                "timestamp": {"$gte": history_start_timestamp, "$lt": cutoff_timestamp},
                "userId": self.user_id,
                "entities": {"$exists": True},
            },
            {"entities": 1, "_id": 0},
        )

        chat_doc_cursor = self.mongodb_community._db["chat_query"].find(
            {
                "_id": self.user_id,
            },
            {"history": 1, "_id": 0},
        )
        chat_doc = list(chat_doc_cursor)

        unique_entities_with_types = set()

        for doc in cursor_1:
            if "entities" in doc and isinstance(doc["entities"], dict):
                for entity, entity_type in doc["entities"].items():
                    if (
                        isinstance(entity, str)
                        and entity
                        and isinstance(entity_type, str)
                        and entity_type
                    ):
                        unique_entities_with_types.add((entity, entity_type))

        if (
            chat_doc
            and "history" in chat_doc[0]
            and isinstance(chat_doc[0]["history"], dict)
        ):
            for time_stamp_str, value in chat_doc[0]["history"].items():
                try:
                    time_stamp = int(time_stamp_str)
                    if (
                        time_stamp >= history_start_timestamp
                        and time_stamp < cutoff_timestamp
                    ):
                        if "entities" in value and isinstance(value["entities"], dict):
                            for entity, entity_type in value["entities"].items():
                                if (
                                    isinstance(entity, str)
                                    and entity
                                    and isinstance(entity_type, str)
                                    and entity_type
                                ):
                                    unique_entities_with_types.add(
                                        (entity, entity_type)
                                    )
                except ValueError:
                    pass

        entities_with_types = list(unique_entities_with_types)

        if not entities_with_types:
            self.user_id = original_user_id
            return 0.0

        interest_prompt, user_interest_embedding = (
            self.create_interest_prompt_and_embedding(entities_with_types)
        )

        if user_interest_embedding is None:
            self.user_id = original_user_id
            return 0.0

        news_query = {
            "publish_date_timestamp": {
                "$gte": cutoff_timestamp,
                "$lt": cutoff_timestamp + 86400 * evaluation_time_window_days,
            },
            "embedding": {"$exists": True},
        }

        news_cursor = self.mongodb_community._db["news_articles"].find(
            news_query, {"embedding": 1}
        )
        news_articles_with_embeddings = list(news_cursor)

        if not news_articles_with_embeddings:
            self.user_id = original_user_id
            return 0.0

        total_similarity = 0.0
        num_articles = 0
        user_embedding_np = np.array(user_interest_embedding)

        for article in news_articles_with_embeddings:
            if "embedding" in article and isinstance(article["embedding"], list):
                try:
                    article_embedding_np = np.array(article["embedding"])
                    if user_embedding_np.shape == article_embedding_np.shape:
                        similarity = np.dot(user_embedding_np, article_embedding_np) / (
                            np.linalg.norm(user_embedding_np)
                            * np.linalg.norm(article_embedding_np)
                        )
                        total_similarity += similarity
                        num_articles += 1
                except Exception:
                    pass

        average_similarity = (
            total_similarity / num_articles if num_articles > 0 else 0.0
        )

        self.user_id = original_user_id

        return average_similarity


if __name__ == "__main__":
    sample_user_id = "0x94A488544C88B9D7640244C1509a14465d647b6A"
    interest_feed_analyzer = GetInterestFeed(user_id=sample_user_id)

    entities_with_types = interest_feed_analyzer.analyze_entity_from_news_chat_history()

    interest_prompt, user_interest_embedding = (
        interest_feed_analyzer.create_interest_prompt_and_embedding(entities_with_types)
    )

    if user_interest_embedding is not None:
        suggested_articles_limited = (
            interest_feed_analyzer.find_similar_news_by_embedding(
                user_interest_embedding, time_window_days=3, limit=5
            )
        )
        print("\nSuggested News Articles (Limited to 5):")
        if suggested_articles_limited:
            for article_data in suggested_articles_limited:
                print(f"  Title: {article_data.get('keyWord')}")
                print("  ---")
        else:
            print("No similar news articles found with the specified limit.")

        suggested_articles_all = interest_feed_analyzer.find_similar_news_by_embedding(
            user_interest_embedding, time_window_days=3
        )
        print("\nSuggested News Articles (All found):")
        if suggested_articles_all:
            print(f"Found {len(suggested_articles_all)} articles.")
        else:
            print("No similar news articles found.")

    else:
        print("Could not generate user interest embedding, skipping news suggestion.")

    print("\n--- Embedding Similarity Evaluation Example ---")
    sample_user_id_eval_sim = "0x94A488544C88B9D7640244C1509a14465d647b6A"
    evaluation_cutoff_timestamp_sim = int(time.time()) - 86400 * 7

    print(
        f"Evaluating embedding similarity for user {sample_user_id_eval_sim} with cutoff timestamp {evaluation_cutoff_timestamp_sim}"
    )

    avg_sim_score = interest_feed_analyzer.evaluate_embedding_similarity(
        sample_user_id_eval_sim,
        evaluation_cutoff_timestamp_sim,
        history_time_window_days=3,
        evaluation_time_window_days=3,
    )

    print(
        f"Average Embedding Similarity for user {sample_user_id_eval_sim}: {avg_sim_score:.4f}"
    )
