import os
import sys

sys.path.append(os.getcwd())

import json
import time

import requests
import torch
from transformers import AutoModelForTokenClassification, AutoTokenizer

from src.constants.llm.agent_prompt import EntityExtractionPromptTemplate
from src.databases.mongodb_community import MongoDBCommunity
from src.services.llm.communication import LLMCommunication
from src.utils.logger import get_logger

logger = get_logger("NER Service")


class NERService:
    def __init__(self):
        self.db = MongoDBCommunity()
        self.llm = LLMCommunication()
        self.model_name = "truongtt/blockchain-ner"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForTokenClassification.from_pretrained(self.model_name)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)

        self.idx2tag_url = (
            "https://huggingface.co/truongtt/blockchain-ner/resolve/main/idx2tag.json"
        )
        self.response = requests.get(self.idx2tag_url)
        self.idx2tag = json.loads(self.response.text)

    def merge_entities(self, entities):
        if not entities:
            return []

        cleaned_entities = []
        seen_tokens = set()

        for token, tag in entities:
            clean_token = token.rstrip(".,!?:;")

            dedup_key = (clean_token, tag)

            if dedup_key not in seen_tokens:
                seen_tokens.add(dedup_key)
                cleaned_entities.append((clean_token, tag))

        merged = []
        current_tokens = []
        current_tag = None

        for token, tag in cleaned_entities:
            entity_type = tag[2:] if tag.startswith(("B-", "I-")) else tag

            if tag.startswith("B-") or (
                current_tag
                and entity_type
                != (
                    current_tag[2:]
                    if current_tag.startswith(("B-", "I-"))
                    else current_tag
                )
            ):
                if current_tokens:
                    final_tag = (
                        current_tag[2:]
                        if current_tag.startswith(("B-", "I-"))
                        else current_tag
                    )
                    merged.append((" ".join(current_tokens), final_tag))

                current_tokens = [token]
                current_tag = tag

            elif tag.startswith("I-"):
                current_tokens.append(token)
            else:
                if current_tokens:
                    final_tag = (
                        current_tag[2:]
                        if current_tag.startswith(("B-", "I-"))
                        else current_tag
                    )
                    merged.append((" ".join(current_tokens), final_tag))
                current_tokens = [token]
                current_tag = tag

        if current_tokens:
            final_tag = (
                current_tag[2:] if current_tag.startswith(("B-", "I-")) else current_tag
            )
            merged.append((" ".join(current_tokens), final_tag))

        return merged

    def extract_entities(self, query):
        ner_prompt_template = EntityExtractionPromptTemplate()
        template = ner_prompt_template.create_template()
        prompt = template.format(text=query)

        response = self.llm.send_prompt(prompt)
        return response

    def process_entities(self, response):
        try:
            # Handle empty or None response
            if not response:
                logger.warning("Empty response from LLM")
                return {}

            # Try to parse as JSON first
            if isinstance(response, str):
                try:
                    # Try to find JSON-like structure in the string
                    start_idx = response.find("{")
                    end_idx = response.rfind("}") + 1
                    if start_idx != -1 and end_idx != 0:
                        json_str = response[start_idx:end_idx]
                        entities = json.loads(json_str)
                    else:
                        logger.error("No valid JSON structure found in response")
                        return {}
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON from response: {e}")
                    return {}
            else:
                entities = response

            # Clean and standardize the entities
            cleaned_entities = {}
            for entity, entity_type in entities.items():
                # Clean entity name
                clean_entity = entity.strip()
                if not clean_entity:
                    continue

                # Clean entity type
                clean_type = entity_type.strip()
                if not clean_type:
                    continue

                # Remove any trailing punctuation from entity name
                clean_entity = clean_entity.rstrip(".,!?:;")

                # Only add if both entity and type are non-empty
                if clean_entity and clean_type:
                    cleaned_entities[clean_entity] = clean_type

            return cleaned_entities

        except Exception as e:
            logger.error(f"Error processing entities: {e}")
            return {}

    def predict_entities(self, text, model, tokenizer):
        tokens = text.split()

        bert_tokens = []
        orig_to_bert_map = []

        for token in tokens:
            orig_to_bert_map.append(len(bert_tokens) + 1)

            subwords = tokenizer.tokenize(token)

            if len(subwords) == 0:
                subwords = ["[UNK]"]

            bert_tokens.extend(subwords)

        bert_tokens = ["[CLS]"] + bert_tokens + ["[SEP]"]

        input_ids = tokenizer.convert_tokens_to_ids(bert_tokens)

        attention_mask = [1] * len(input_ids)

        max_len = 128
        padding_length = max_len - len(input_ids)

        if padding_length > 0:
            input_ids = input_ids + ([0] * padding_length)
            attention_mask = attention_mask + ([0] * padding_length)
        else:
            input_ids = input_ids[:max_len]
            attention_mask = attention_mask[:max_len]

        input_ids = torch.tensor([input_ids], dtype=torch.long).to(self.device)
        attention_mask = torch.tensor([attention_mask], dtype=torch.long).to(
            self.device
        )

        model.eval()
        with torch.no_grad():
            outputs = model(input_ids=input_ids, attention_mask=attention_mask)
            predictions = torch.argmax(outputs.logits, dim=2)

        pred_tags = []
        for p in predictions[0]:
            idx = p.item()
            if str(idx) in self.idx2tag:
                pred_tags.append(self.idx2tag[str(idx)])
            else:
                logger.warning(f"Warning: Label index {idx} not found in mapping")
                pred_tags.append("O")

        result = []
        for i, token in enumerate(tokens):
            bert_idx = orig_to_bert_map[i]
            if bert_idx < len(pred_tags):
                pred_tag = pred_tags[bert_idx]
                if pred_tag != "O" and pred_tag != "X":
                    result.append((token, pred_tag))

        # Gộp các entities liên tiếp
        return self.merge_entities(result)

    def predict_entities_from_db(self):
        news_docs = self.db.get_collection("news_articles").find(
            {
                "entities": {"$exists": False},
                "publish_date_timestamp": {"$gte": int(time.time() - 86400)},
            }
        )
        news_docs = list(news_docs)
        logger.info(f"Predicting entities from {len(news_docs)} news articles")
        for doc in news_docs:
            try:
                predicted_entities = self.predict_entities(
                    doc["summary"], self.model, self.tokenizer
                )
                entities = {}
                for entity, tag in predicted_entities:
                    entities[entity] = tag

                doc["entities"] = entities
                self.db.get_collection("news_articles").update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"entities": entities}},
                )
            except Exception as e:
                logger.error(
                    f"Error predicting entities from news article {doc['_id']}: {e}"
                )

        analytics_docs = self.db.get_collection("analytics").find(
            {
                "entities": {"$exists": False},
                "lastUpdated": {"$gte": int(time.time() - 86400)},
            }
        )
        analytics_docs = list(analytics_docs)
        logger.info(f"Predicting entities from {len(analytics_docs)} analytics")
        for doc in analytics_docs:
            try:
                predicted_entities = self.predict_entities(
                    doc["analysis"], self.model, self.tokenizer
                )
                entities = {}
                for entity, tag in predicted_entities:
                    entities[entity] = tag

                doc["entities"] = entities
                self.db.get_collection("analytics").update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"entities": entities}},
                )
            except Exception as e:
                logger.error(
                    f"Error predicting entities from analytics {doc['_id']}: {e}"
                )


if __name__ == "__main__":
    ner_service = NERService()
    response = ner_service.extract_entities(
        """Unstaked redefines that playbook with autonomous AI agents that engage users, drive conversations, and reward participation across platforms like X and Telegram. These AI agents do more than schedule posts or send basic replies. For those seeking long-term presence instead of short-term noise, Unstaked delivers a smarter way to grow. With its AI agents and on-chain performance tracking, Unstaked ($UNSD) offers a model that is efficient, transparent, and already attracting serious interest, including millions raised during presale. For those focused on long-term utility over speculation, Unstaked stands out as a crypto project built for lasting relevance.
"""
    )
    # print(response)
    entities = ner_service.process_entities(response)
    print(entities)
