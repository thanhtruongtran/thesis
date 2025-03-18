import os
import random
import sys
import time

sys.path.append(os.getcwd())

from typing import Any, Dict, List

from dotenv import load_dotenv
from openai import AzureOpenAI
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import PointStruct

from src.databases.mongodb_cdp import MongoDBCDP
from src.utils.logger import get_logger
from src.utils.text import data_process

load_dotenv()
logger = get_logger(__name__)


class QdrantDB:
    def __init__(self):
        self.client = QdrantClient(
            os.getenv("QDRANT_HOST"),
            port=os.getenv("QDRANT_PORT"),
            timeout=600,
            api_key=os.getenv("QDRANT_API_KEY"),
        )
        logger.info("Client created...")
        self.client_azure_openai = AzureOpenAI(
            api_key=os.getenv("EMBEDDING_OPENAI_API"),
            api_version=os.getenv("EMBEDDING_API_VERSION"),
            azure_endpoint=os.getenv("EMBEDDING_AZURE_ENDPOINT"),
        )
        self.mongodb_cdp = MongoDBCDP()

    def create_collection(self, collection_name):
        try:
            self.client.get_collection(collection_name)
            logger.info(f"Collection {collection_name} already exists.")
        except Exception:
            logger.info(f"Creating collection {collection_name}...")
            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=models.VectorParams(
                    size=1536,
                    distance=models.Distance.COSINE,
                ),
                on_disk_payload=True,
                optimizers_config=models.OptimizersConfigDiff(
                    memmap_threshold=200000,
                ),
                hnsw_config=models.HnswConfigDiff(on_disk=True),
            )
            logger.info(f"Collection {collection_name} created successfully.")

    def delete_collection(self, collection_name):
        logger.info(f"Deleting collection {collection_name}...")
        self.client.delete_collection(collection_name=collection_name)

    def get_embeddings(self, text: str):
        processed_text = data_process(text)[0]

        try:
            response = self.client_azure_openai.embeddings.create(
                input=processed_text,
                model="text-embedding-3-small",
            )
            embed = response.data[0].embedding
            return embed
        except Exception as e:
            logger.error(f"Error getting embeddings: {e}")
            return

    def get_embeddings_with_fallback(service, texts):
        embeddings = []
        for text in texts:
            embedding = service.get_embeddings(text)
            if embedding:
                embeddings.append(embedding)
            else:
                embeddings.append([0] * 1536)

        return embeddings

    def upload_documents(
        self, collection_name: str, documents: List[Dict[str, Any]], batch_size
    ):
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            texts = [doc["text"] for doc in batch]

            try:
                embeddings = self.get_embeddings_with_fallback(texts)
                points = []
                for doc, embedding in zip(batch, embeddings):
                    try:
                        id = int(doc["_id"].split("_")[-1])
                        points.append(
                            PointStruct(
                                id=id,
                                vector=embedding,
                                payload={
                                    "keyword": doc.get("project_id", ""),
                                    "text": doc.get("text"),
                                    "datetime": int(doc.get("timestamp")),
                                },
                            )
                        )
                    except Exception as doc_err:
                        logger.error(
                            f"Error processing document {doc.get('_id')}: {doc_err}"
                        )

                if points:
                    self.client.upsert(
                        collection_name=collection_name, wait=True, points=points
                    )
                # logger.info(f"Uploaded {len(points)} documents.")

            except Exception as batch_err:
                logger.error(f"Error processing batch: {batch_err}")

    def get_projects(self):
        projects_id_twitter = []
        cursor = self.mongodb_cdp._db["projects_social_media"].find(
            {"twitter": {"$exists": True}}
        )
        for project in cursor:
            projects_id_twitter.append(
                {
                    "project_id": project["_id"],
                    "twitter_id": project["twitter"].get("id"),
                }
            )

        return projects_id_twitter

    def get_kols(self):
        kols = []
        cursor = self.mongodb_cdp._db["twitter_raw"].find({"type": "kol"})
        for kol in cursor:
            kols.append(kol.get("userName", ""))

        return kols

    def get_twitter_documents(self, project_id, period):
        tweet_documents = []
        reupload_tweet_documents = []
        cursor = self.mongodb_cdp._db["tweets"].find(
            {
                "authorName": project_id,
                "timestamp": {"$gt": time.time() - period},
                "qdrantUploaded": {"$exists": False},
            }
        )
        for document in cursor:
            if document.get("text"):
                if (
                    "https" in document.get("text")
                    and len(document.get("text").strip().split()) == 1
                ):
                    continue
                else:
                    document["project_id"] = document.get("authorName")
                    tweet_documents.append(document)

                    ## Reupload
                    reup_doc = {}
                    reup_doc["qdrantUploaded"] = True
                    reup_doc["_id"] = document["_id"]
                    reupload_tweet_documents.append(reup_doc)

        if len(reupload_tweet_documents) > 0:
            self.mongodb_cdp.update_docs(
                collection_name="tweets", data=reupload_tweet_documents, merge=True
            )

        return tweet_documents

    def get_twitter_documents_to_clean(self, project_id, period):
        tweet_documents = []
        reupload_tweet_documents = []
        cursor = self.mongodb_cdp._db["tweets"].find(
            {
                "authorName": project_id,
                "timestamp": {"$gt": time.time() - period},
                "qdrantCleanedUploaded": {"$exists": False},
            }
        )
        for document in cursor:
            if document.get("text"):
                if (
                    "https" in document.get("text")
                    and len(document.get("text").strip().split()) == 1
                ):
                    continue
                else:
                    document["project_id"] = document.get("authorName")
                    tweet_documents.append(document)

                    ## Reupload
                    reup_doc = {}
                    reup_doc["qdrantUploaded"] = True
                    reup_doc["_id"] = document["_id"]
                    reupload_tweet_documents.append(reup_doc)

        if len(reupload_tweet_documents) > 0:
            self.mongodb_cdp.update_docs(
                collection_name="tweets", data=reupload_tweet_documents, merge=True
            )

        return tweet_documents

    def get_telegram_documents(self, period):
        telegram_documents = []
        cursor = self.mongodb_cdp._db["telegram_messages"].find(
            {"announcement": True, "timestamp": {"$gt": time.time() - period}}
        )
        for document in cursor:
            if document.get("message"):
                if (
                    "https" in document.get("message")
                    and len(document.get("message").strip().split()) == 1
                ):
                    continue
                else:
                    document["text"] = document.get("message")
                    document["project_id"] = document.get("project")
                    telegram_documents.append(document)

        return telegram_documents

    def upload_twitter_documents(self, batch_size, period):
        collection_name = "twitter_top_project_lemma"
        collection_name_cleaned = "twitter_top_project_lemma_cleaned"
        self.create_collection(collection_name)
        self.create_collection(collection_name_cleaned)

        projects = self.get_projects()

        cnt = 1
        for prj in projects:
            _, twitter_id = prj["project_id"], prj["twitter_id"]
            tweet_documents = self.get_twitter_documents(
                project_id=twitter_id, period=period
            )
            n = len(tweet_documents)
            logger.info(f"Uploading {n} documents of project {twitter_id}...")
            for i in range(0, n, batch_size):
                self.upload_documents(
                    collection_name,
                    tweet_documents[i : i + batch_size],
                    batch_size=batch_size,
                )

            tweet_documents_cleaned = self.get_twitter_documents_to_clean(
                project_id=twitter_id, period=period
            )
            tweet_documents_cleaned = [
                doc for doc in tweet_documents_cleaned if doc.get("lang", "") == "en"
            ]
            for doc in tweet_documents_cleaned:
                if "https" in doc.get("text"):
                    doc["text"] = " ".join(
                        [
                            word
                            for word in doc.get("text").split()
                            if "https" not in word
                        ]
                    )
            tweet_documents_cleaned = [
                doc for doc in tweet_documents_cleaned if len(doc.get("text", "")) > 50
            ]

            n = len(tweet_documents_cleaned)
            logger.info(f"Uploading {n} cleaned documents of project {twitter_id}...")
            for i in range(0, n, batch_size):
                self.upload_documents(
                    collection_name_cleaned,
                    tweet_documents_cleaned[i : i + batch_size],
                    batch_size=batch_size,
                )

            cnt += 1
            if cnt % 500 == 0:
                logger.info(f"Uploaded {cnt}/{len(projects)} projects.")

        logger.info(f"Uploaded {len(projects)} projects.")

        kols = self.get_kols()
        cnt = 1
        for kol in kols:
            tweet_documents = self.get_twitter_documents(project_id=kol, period=period)
            n = len(tweet_documents)
            logger.info(f"Uploading {n} documents of kol {kol}...")
            for i in range(0, n, batch_size):
                self.upload_documents(
                    collection_name,
                    tweet_documents[i : i + batch_size],
                    batch_size=batch_size,
                )

            tweet_documents_cleaned = self.get_twitter_documents_to_clean(
                project_id=kol, period=period
            )
            tweet_documents_cleaned = [
                doc for doc in tweet_documents if doc.get("lang", "") == "en"
            ]
            for doc in tweet_documents_cleaned:
                if "https" in doc.get("text"):
                    doc["text"] = " ".join(
                        [
                            word
                            for word in doc.get("text").split()
                            if "https" not in word
                        ]
                    )
            tweet_documents_cleaned = [
                doc for doc in tweet_documents_cleaned if len(doc.get("text", "")) > 50
            ]

            n = len(tweet_documents_cleaned)
            logger.info(f"Uploading {n} cleaned documents of kol {kol}...")
            for i in range(0, n, batch_size):
                self.upload_documents(
                    collection_name_cleaned,
                    tweet_documents_cleaned[i : i + batch_size],
                    batch_size=batch_size,
                )

            cnt += 1
            if cnt % 100 == 0:
                logger.info(f"Uploaded {cnt}/{len(kols)} kols.")

        logger.info(f"Uploaded {len(kols)} kols.")

    def get_vectors_by_ids(
        self,
        tweet_ids: list[int],
        batch_size: int = 500,
        retries: int = 3,
    ):
        vectors = []
        for i in range(0, len(tweet_ids), batch_size):
            batch = tweet_ids[i : i + batch_size]
            retry_attempts = 0

            while retry_attempts < retries:
                try:
                    # Retrieve data
                    batch_responses = self.client.retrieve(
                        collection_name="twitter_top_project_lemma",
                        ids=batch,
                        with_vectors=True,
                        with_payload=True,
                    )
                    if not batch_responses:
                        logger.warning(f"No response for batch {i // batch_size + 1}")
                        self.client = QdrantClient(
                            os.getenv("QDRANT_HOST"),
                            port=os.getenv("QDRANT_PORT"),
                            timeout=600,
                            api_key=os.getenv("QDRANT_API_KEY"),
                        )
                        break

                    batch_vectors = [
                        {
                            "_id": response.id,
                            "embedding": response.vector,
                            "text": response.payload.get("text"),
                            "timestamp": response.payload.get("datetime"),
                        }
                        for response in batch_responses
                        if response.vector and response.id
                    ]
                    vectors.extend(batch_vectors)
                    logger.info(f"Retrieved {len(vectors)} / {len(tweet_ids)} points.")
                    break
                except Exception as e:
                    self.client = QdrantClient(
                        os.getenv("QDRANT_HOST"),
                        port=os.getenv("QDRANT_PORT"),
                        timeout=600,
                        api_key=os.getenv("QDRANT_API_KEY"),
                    )
                    retry_attempts += 1
                    logger.warning(
                        f"Error for batch {i // batch_size + 1} (attempt {retry_attempts}): {e}"
                    )
                    if retry_attempts == retries:
                        logger.error(
                            f"Failed after {retries} attempts for batch {i // batch_size + 1}"
                        )
                        break
                    continue
            time.sleep(2)
        logger.info(f"Retrieved {len(vectors)} / {len(tweet_ids)} points.")
        return vectors

    def upload_telegram_documents(self, batch_size, period):
        collection_name = "telegram_top_project_lemma"
        self.create_collection(collection_name)

        telegram_documents = self.get_telegram_documents(period=period)

        n = len(telegram_documents)
        for i in range(0, n, batch_size):
            self.upload_documents(
                collection_name,
                telegram_documents[i : i + batch_size],
                batch_size=batch_size,
            )
            tmp = min(i + batch_size, n)
            logger.info(f"Uploaded {tmp}/{n} documents of telegram.")

        logger.info(f"Finished uploading {n} telegram documents.")

    def query(self, collection_name, text, num_hits):
        text = data_process(text)[0]
        query_embedding = self.get_embeddings([text])
        search_results = self.client.search(
            collection_name=collection_name,
            query_vector=query_embedding,
            limit=num_hits,
        )

        return search_results

    # query by keyword field in the payload
    def query_by_keyword(self, collection_name, keyword):
        search_results = []
        try:
            match_documents = self.client.scroll(
                collection_name=collection_name,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="keyword", match=models.MatchValue(value=keyword)
                        )
                    ]
                ),
            )
            match_documents = match_documents[0]
            idx = random.choice(range(len(match_documents)))
            tmp = match_documents[idx]
            search_results.append(tmp.payload.get("text"))

        except Exception:
            logger.error(
                f"Error occurred while querying {collection_name} by keyword {keyword}."
            )

        return search_results

    def count_documents(self, collection_name, period):
        timestamp = time.time() - period
        timestamp = int(timestamp)
        count = self.client.count(
            collection_name=collection_name,
            exact=True,
            timeout=300,
            count_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="datetime", range=models.Range(lte=timestamp)
                    )
                ]
            ),
        )

        return count.count

    def prune_documents(self, collection_name, period):
        timestamp = time.time() - period
        timestamp = int(timestamp)
        total_limit = self.count_documents(collection_name, period)
        logger.info(f"Total documents to be pruned: {total_limit}")

        batch_limit = 100
        total_fetched = 0
        all_match_documents = []
        while total_fetched < total_limit:
            time.sleep(60)
            list_points = []
            match_documents, offset = self.client.scroll(
                collection_name=collection_name,
                limit=batch_limit,
                scroll_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="datetime", range=models.Range(lt=timestamp)
                        )
                    ]
                ),
            )

            if not match_documents:
                break

            all_match_documents.extend(match_documents)
            total_fetched += batch_limit

            for document in match_documents:
                list_points.append(document.id)

            try:
                self.client.delete(
                    collection_name=collection_name,
                    points_selector=models.PointIdsList(points=list_points),
                )
                logger.info(f"Pruned {len(list_points)} documents.")

            except Exception:
                logger.error(
                    f"Error occurred while pruning {collection_name} within period {period} days."
                )
                continue

        # prune documents that have same text in all_match_documents
        unique_texts = set()
        for document in all_match_documents:
            try:
                text = document.payload.get("text")

                if text not in unique_texts:
                    unique_texts.add(text)
                else:
                    try:
                        self.client.delete(
                            collection_name=collection_name,
                            points_selector=models.PointIdsList(points=[document.id]),
                        )
                        total_fetched += 1
                        logger.info("Pruned document with duplicate text.")
                    except Exception:
                        continue
            except Exception:
                continue

        logger.info(f"Pruned {total_fetched} documents in total.")
