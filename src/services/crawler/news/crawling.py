import os
import sys
import time

sys.path.append(os.getcwd())
import xml.etree.ElementTree as ET

import feedparser
import requests
from cli_scheduler.scheduler_job import SchedulerJob
from newspaper import Article
from sentence_transformers import SentenceTransformer

from src.constants.time import TimeConstants
from src.databases.mongodb_community import MongoDBCommunity
from src.databases.mongodb_klg import MongoDBKLG
from src.services.crawler.news.config import BASE_URLS, BASE_URLS_V2
from src.utils.logger import get_logger
from src.utils.time import round_timestamp

logger = get_logger("News Crawling")


class NewsCrawling(SchedulerJob):
    def __init__(self, interval, delay, run_now, time_interval):
        scheduler = f"^{run_now}@{interval}/{delay}#true"
        super().__init__(scheduler)
        self.mongodb = MongoDBCommunity()
        self.mongodb_klg = MongoDBKLG()
        self.base_urls = BASE_URLS
        self.base_urls_v2 = BASE_URLS_V2
        self.time_interval = time_interval
        self.model = SentenceTransformer("all-MiniLM-L6-v2")

    def _start(self):
        self.all_assets = self.mongodb_klg.get_tokens("smart_contracts")

    def check_assets_from_news(self, text):
        list_assets = []
        list_words = text.lower().split(" ")
        for word in list_words:
            if word in self.all_assets:
                list_assets.append(word.upper())

        return list_assets

    def get_articles_from_feed(self, feed_url):
        feed = feedparser.parse(feed_url)
        list_news = []
        start_time = int(time.time()) - TimeConstants.A_DAY * self.time_interval
        start_time = round_timestamp(start_time)

        for entry in feed.entries:
            url = entry.link
            try:
                article = Article(url)
                article.download()
                article.parse()
                article.nlp()

                publish_date = article.publish_date
                publish_date_timestamp = int(publish_date.timestamp())
                if publish_date_timestamp < start_time:
                    continue

                list_assets = self.check_assets_from_news(article.text)

                # Generate embedding and convert to list
                text_for_embedding = article.title + " " + article.summary
                embedding = self.model.encode(
                    text_for_embedding, convert_to_tensor=True
                )
                embedding_list = embedding.cpu().numpy().tolist()

                list_news.append(
                    {
                        "_id": article.url,
                        "title": article.title,
                        "url": article.url,
                        "text": article.text,
                        "publish_date_timestamp": publish_date_timestamp,
                        "publish_date": str(article.publish_date),
                        "summary": article.summary,
                        "keywords": article.keywords,
                        "img_url": article.top_image,
                        "type": "news",
                        "assets": list_assets,
                        "embedding": embedding_list,
                    }
                )
            except Exception as e:
                logger.error(f"Error while parsing {url}: {str(e)}")

        return list_news

    def get_articles_from_feed_v2(self, feed_url):
        list_news = []

        try:
            response = requests.get(feed_url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch feed from {feed_url}: {str(e)}")
            return list_news

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse XML from {feed_url}: {str(e)}")
            return list_news

        start_time = int(time.time()) - TimeConstants.A_DAY * self.time_interval
        start_time = round_timestamp(start_time)

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item")
        if not items:
            items = root.findall("atom:entry", ns)

        for item in items:
            try:
                url = item.findtext("link")
                if url is None:
                    link_elem = item.find("atom:link", ns)
                    url = (
                        link_elem.attrib.get("href") if link_elem is not None else None
                    )

                publish_date = item.findtext("pubDate")
                if publish_date is None:
                    publish_date = item.findtext("atom:updated", namespaces=ns)

                if not url or not publish_date:
                    continue

                try:
                    publish_date_timestamp = int(
                        time.mktime(
                            time.strptime(publish_date, "%a, %d %b %Y %H:%M:%S %z")
                        )
                    )
                except ValueError:
                    try:
                        publish_date_timestamp = int(
                            time.mktime(
                                time.strptime(publish_date, "%Y-%m-%dT%H:%M:%S.%fZ")
                            )
                        )
                    except ValueError:
                        logger.warning(f"Unrecognized date format: {publish_date}")
                        continue

                if publish_date_timestamp < start_time:
                    continue

                try:
                    article = Article(url)
                    article.download()
                    article.parse()
                    article.nlp()

                    list_assets = self.check_assets_from_news(article.text)

                    # Generate embedding and convert to list
                    text_for_embedding = article.title + " " + article.summary
                    embedding = self.model.encode(
                        text_for_embedding, convert_to_tensor=True
                    )
                    embedding_list = embedding.cpu().numpy().tolist()

                    list_news.append(
                        {
                            "_id": article.url,
                            "title": article.title,
                            "url": article.url,
                            "text": article.text,
                            "publish_date_timestamp": publish_date_timestamp,
                            "publish_date": publish_date,
                            "summary": article.summary,
                            "keywords": article.keywords,
                            "img_url": article.top_image,
                            "type": "news",
                            "assets": list_assets,
                            "embedding": embedding_list,
                        }
                    )

                except Exception as e:
                    logger.error(f"Error while parsing article at {url}: {str(e)}")

            except Exception as e:
                logger.error(f"Unexpected error while processing item: {str(e)}")

        return list_news

    def _execute(self, *args, **kwargs):
        for feed_url in self.base_urls:
            articles = self.get_articles_from_feed(feed_url)
            self.mongodb.update_docs(
                collection_name="news_articles",
                data=articles,
            )
            logger.info(f"Inserted {len(articles)} articles from {feed_url}")

        for feed_url in self.base_urls_v2:
            articles = self.get_articles_from_feed_v2(feed_url)
            self.mongodb.update_docs(
                collection_name="news_articles",
                data=articles,
            )
            logger.info(f"Inserted {len(articles)} articles from {feed_url}")

        logger.info("News crawling completed.")
