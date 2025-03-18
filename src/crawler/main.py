import asyncio
import os
import sys
sys.path.append(os.getcwd())

from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv

from src.crawler.config import BASE_URLS, CSS_SELECTOR, REQUIRED_KEYS
from src.crawler.utils.data_utils import save_newss_to_csv
from src.crawler.utils.scraper_utils import (
    fetch_and_process_page,
    get_browser_config,
    get_llm_strategy,
)

load_dotenv()


async def crawl_newss():
    """
    Main function to crawl news data from multiple websites.
    """
    # Initialize configurations
    browser_config = get_browser_config()
    llm_strategy = get_llm_strategy()
    session_id = "news_crawl_session"

    all_newss = []

    async with AsyncWebCrawler(config=browser_config) as crawler:
        for base_url in BASE_URLS[1:2]:
            print(f"Starting crawl for: {base_url}")

            # Initialize state variables
            page_number = 1
            seen_names = set()

            while page_number < 2:  # Giới hạn 2 trang
                newss, no_results_found = await fetch_and_process_page(
                    crawler,
                    page_number,
                    base_url,
                    CSS_SELECTOR,
                    llm_strategy,
                    session_id,
                    REQUIRED_KEYS,
                    seen_names,
                )

                if no_results_found:
                    print(f"No more newss found on {base_url}. Ending crawl for this site.")
                    break

                if not newss:
                    print(f"No newss extracted from page {page_number} on {base_url}.")
                    break

                all_newss.extend(newss)
                page_number += 1

                await asyncio.sleep(2)

            print(f"Completed crawling for {base_url}. Waiting 1 minute before next site.")
            await asyncio.sleep(60)

    if all_newss:
        save_newss_to_csv(all_newss, "complete_newss.csv")
        print(f"Saved {len(all_newss)} newss to 'complete_newss.csv'.")
    else:
        print("No newss were found during the crawl.")

    llm_strategy.show_usage()


async def main():
    """
    Entry point of the script.
    """
    await crawl_newss()


if __name__ == "__main__":
    asyncio.run(main())
