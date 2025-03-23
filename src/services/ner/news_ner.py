import os
import sys
sys.path.append(os.getcwd())
import json
import pandas as pd
import re

from src.databases.mongodb_community import MongoDBCommunity
from src.utils.logger import get_logger

logger = get_logger("Entity Extractor")


class EntityExtractor:
    def __init__(self, chains_path, projects_path, tokens_path, trends_path):
        self.chains_dict = self._load_json(chains_path)
        self.projects_list = self._load_json(projects_path)
        self.tokens_list = self._load_json(tokens_path)
        self.trends_dict = self._load_json(trends_path)
        self.mongodb = MongoDBCommunity()

    def _load_json(self, path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _normalize(self, text):
        return text.lower()

    def _match_keywords(self, text, keyword_list):
        """
        Kiểm tra xem có keyword nào xuất hiện trong text với điều kiện whole-word
        """
        text = self._normalize(text)
        matches = set()
        for kw in keyword_list:
            pattern = r'\b' + re.escape(kw.lower()) + r'\b'
            if re.search(pattern, text):
                matches.add(kw)
        return matches

    def extract(self, text):
        found_chains = set()
        found_projects = set()
        found_tokens = set()
        found_trends = set()
        found_tags = set()

        # CHAIN
        for chain, keywords in self.chains_dict.items():
            if self._match_keywords(text, keywords):
                found_chains.add(chain)

        # PROJECT
        for project in self.projects_list:
            name = project.get("name", "")
            if self._match_keywords(text, [name]):
                found_projects.add(name)
                if "category" in project:
                    found_tags.add(project["category"])

        # # TOKEN
        # for token in self.tokens_list:
        #     name = token.get("name", "")
        #     symbol = token.get("symbol", "")
        #     keywords = [kw for kw in [name, symbol] if kw]
        #     if self._match_keywords(text, keywords):
        #         found_tokens.add(symbol or name)
        #         found_tags.update(token.get("categories", []))

        # TREND
        for trend, keywords in self.trends_dict.items():
            if self._match_keywords(text, keywords):
                found_trends.add(trend)

        return pd.DataFrame([{
            "text": text,
            "CHAIN": sorted(list(found_chains)),
            "PROJECT": sorted(list(found_projects)),
            "TOKEN": sorted(list(found_tokens)),
            "TREND": sorted(list(found_trends)),
            "tags": sorted(list(found_tags)),
        }])
    
    def extract_from_news(self):
        news_articles = self.mongodb._db["news_articles"].find()
        news_articles = list(news_articles)[:10]
        list_text = [news["summary"] for news in news_articles]
        df = pd.DataFrame()

        for text in list_text:
            df = pd.concat([df, self.extract(text)], ignore_index=True)

        return df
    

if __name__ == "__main__":
    chains_path = "src/data/chains.json"
    projects_path = "src/data/projects.json"
    tokens_path = "src/data/tokens.json"
    trends_path = "src/data/trends.json"

    entity_extractor = EntityExtractor(chains_path, projects_path, tokens_path, trends_path)
    df = entity_extractor.extract_from_news()
    print(df)
    df.to_csv("news_entities.csv", index=False)