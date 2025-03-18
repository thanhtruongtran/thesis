# import json
# import os
# from typing import List, Set, Tuple

# from crawl4ai import (
#     AsyncWebCrawler,
#     BrowserConfig,
#     CacheMode,
#     CrawlerRunConfig,
#     LLMExtractionStrategy,
# )

# from src.crawler.models.news import News
# from src.crawler.utils.data_utils import is_complete_news, is_duplicate_news


# def get_browser_config() -> BrowserConfig:
#     """
#     Returns the browser configuration for the crawler.

#     Returns:
#         BrowserConfig: The configuration settings for the browser.
#     """
#     # https://docs.crawl4ai.com/core/browser-crawler-config/
#     return BrowserConfig(
#         browser_type="chromium",  # Type of browser to simulate
#         headless=False,  # Whether to run in headless mode (no GUI)
#         verbose=True,  # Enable verbose logging
#     )


# def get_llm_strategy() -> LLMExtractionStrategy:
#     """
#     Returns the configuration for the language model extraction strategy.

#     Returns:
#         LLMExtractionStrategy: The settings for how to extract data using LLM.
#     """
#     # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
#     return LLMExtractionStrategy(
#         provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
#         api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
#         schema=News.model_json_schema(),  # JSON schema of the data model
#         extraction_type="schema",  # Type of extraction to perform
#         instruction=(
#             "Extract all news objects with 'title', 'link', 'publishedTime', 'category', "
#             "'description', 'content'  from the following content."
#         ),  # Instructions for the LLM
#         input_format="xml",  # Format of the input data
#         verbose=True,  # Enable verbose logging
#     )


# async def check_no_results(
#     crawler: AsyncWebCrawler,
#     url: str,
#     session_id: str,
# ) -> bool:
#     """
#     Checks if the "No Results Found" message is present on the page.

#     Args:
#         crawler (AsyncWebCrawler): The web crawler instance.
#         url (str): The URL to check.
#         session_id (str): The session identifier.

#     Returns:
#         bool: True if "No Results Found" message is found, False otherwise.
#     """
#     # Fetch the page without any CSS selector or extraction strategy
#     result = await crawler.arun(
#         url=url,
#         config=CrawlerRunConfig(
#             cache_mode=CacheMode.BYPASS,
#             session_id=session_id,
#         ),
#     )

#     if result.success:
#         if "No Results Found" in result.cleaned_html:
#             return True
#     else:
#         print(
#             f"Error fetching page for 'No Results Found' check: {result.error_message}"
#         )

#     return False


# async def fetch_and_process_page(
#     crawler: AsyncWebCrawler,
#     page_number: int,
#     base_url: str,
#     css_selector: str,
#     llm_strategy: LLMExtractionStrategy,
#     session_id: str,
#     required_keys: List[str],
#     seen_names: Set[str],
# ) -> Tuple[List[dict], bool]:
#     """
#     Fetches and processes a single page of news data.

#     Args:
#         crawler (AsyncWebCrawler): The web crawler instance.
#         page_number (int): The page number to fetch.
#         base_url (str): The base URL of the website.
#         css_selector (str): The CSS selector to target the content.
#         llm_strategy (LLMExtractionStrategy): The LLM extraction strategy.
#         session_id (str): The session identifier.
#         required_keys (List[str]): List of required keys in the news data.
#         seen_names (Set[str]): Set of news names that have already been seen.

#     Returns:
#         Tuple[List[dict], bool]:
#             - List[dict]: A list of processed newss from the page.
#             - bool: A flag indicating if the "No Results Found" message was encountered.
#     """
#     url = f"{base_url}?page={page_number}"
#     print(f"Loading page {page_number}...")

#     # Check if "No Results Found" message is present
#     no_results = await check_no_results(crawler, url, session_id)
#     if no_results:
#         return [], True  # No more results, signal to stop crawling

#     # Fetch page content with the extraction strategy
#     result = await crawler.arun(
#         url=url,
#         config=CrawlerRunConfig(
#             cache_mode=CacheMode.BYPASS,  # Do not use cached data
#             extraction_strategy=llm_strategy,  # Strategy for data extraction
#             css_selector=css_selector,  # Target specific content on the page
#             session_id=session_id,  # Unique session ID for the crawl
#         ),
#     )

#     if not (result.success and result.extracted_content):
#         print(f"Error fetching page {page_number}: {result.error_message}")
#         return [], False

#     # Parse extracted content
#     extracted_data = json.loads(result.extracted_content)
#     if not extracted_data:
#         print(f"No news found on page {page_number}.")
#         return [], False

#     # After parsing extracted content
#     print("Extracted data:", extracted_data)

#     # Process newss
#     complete_newss = []
#     for news in extracted_data:
#         # Debugging: Print each news to understand its structure
#         print("Processing news:", news)

#         # Ignore the 'error' key if it's False
#         if news.get("error") is False:
#             news.pop("error", None)  # Remove the 'error' key if it's False

#         if not is_complete_news(news, required_keys):
#             continue  # Skip incomplete newss

#         if is_duplicate_news(news["title"], seen_names):
#             print(f"Duplicate news '{news['title']}' found. Skipping.")
#             continue  # Skip duplicate newss

#         # Add news to the list
#         seen_names.add(news["title"])
#         complete_newss.append(news)

#     if not complete_newss:
#         print(f"No complete newss found on page {page_number}.")
#         return [], False

#     print(f"Extracted {len(complete_newss)} newss from page {page_number}.")
#     return complete_newss, False  # Continue crawling


# async def process_content(crawler, content, extraction_strategy):
#     """
#     Process HTML content with the given extraction strategy.
    
#     Args:
#         crawler: The web crawler instance.
#         content: The HTML content to process.
#         extraction_strategy: The extraction strategy to use.
        
#     Returns:
#         The extraction result.
#     """
#     return await crawler.process_content(
#         content=content,
#         extraction_strategy=extraction_strategy,
#     )

import json
import os
import re
import asyncio
from typing import List, Set, Tuple, Dict, Any
from dotenv import load_dotenv
import tempfile

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
)

from src.crawler.models.news import News
from src.crawler.utils.data_utils import is_complete_news, is_duplicate_news


load_dotenv()
os.environ["AZURE_API_KEY"] = os.getenv("OPENAI_API")
os.environ["AZURE_API_BASE"] = os.getenv("AZURE_ENDPOINT")
os.environ["AZURE_API_VERSION"] = os.getenv("API_VERSION")


def get_browser_config() -> BrowserConfig:
    """
    Returns the browser configuration for the crawler.

    Returns:
        BrowserConfig: The configuration settings for the browser.
    """
    # https://docs.crawl4ai.com/core/browser-crawler-config/
    return BrowserConfig(
        browser_type="chromium",  # Type of browser to simulate
        headless=False,  # Whether to run in headless mode (no GUI)
        verbose=True,  # Enable verbose logging
    )


def get_llm_strategy() -> LLMExtractionStrategy:
    """
    Returns the configuration for the language model extraction strategy.

    Returns:
        LLMExtractionStrategy: The settings for how to extract data using LLM.
    """
    # https://docs.crawl4ai.com/api/strategies/#llmextractionstrategy
    return LLMExtractionStrategy(
        # provider="groq/deepseek-r1-distill-llama-70b",  # Name of the LLM provider
        # api_token=os.getenv("GROQ_API_KEY"),  # API token for authentication
        provider="azure/gpt-3.5",
        api_base=os.environ["AZURE_API_BASE"],
        api_token=os.environ["AZURE_API_KEY"],
        schema=News.model_json_schema(),  # JSON schema of the data model
        extraction_type="schema",  # Type of extraction to perform
        instruction=(
            """
            - Extract all news objects with: 'title', 'link', 'publishedTime', 'category', 'description', 'content'  
            from the following content.
            - If any of the required fields are missing, let it be an empty string, do not contain any information.
            """
        ),  # Instructions for the LLM
        input_format="xml",  # Format of the input data
        verbose=True,  # Enable verbose logging
    )


async def check_no_results(
    crawler: AsyncWebCrawler,
    url: str,
    session_id: str,
) -> bool:
    """
    Checks if the "No Results Found" message is present on the page.

    Args:
        crawler (AsyncWebCrawler): The web crawler instance.
        url (str): The URL to check.
        session_id (str): The session identifier.

    Returns:
        bool: True if "No Results Found" message is found, False otherwise.
    """
    # Fetch the page without any CSS selector or extraction strategy
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )

    if result.success:
        if "No Results Found" in result.cleaned_html:
            return True
    else:
        print(
            f"Error fetching page for 'No Results Found' check: {result.error_message}"
        )

    return False


async def process_with_token_limit(
    crawler: AsyncWebCrawler,
    content: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    max_tokens: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Process content while respecting token limits by creating temporary HTML files.
    
    Args:
        crawler: The web crawler instance.
        content: The content to process.
        llm_strategy: The extraction strategy to use.
        session_id: Session ID for the crawler.
        max_tokens: Maximum tokens allowed per request.
        
    Returns:
        List of extracted items.
    """
    # Roughly estimate token count (4 chars ≈ 1 token)
    estimated_tokens = len(content) // 4
    
    if estimated_tokens <= max_tokens:
        # Content is within token limit, process normally
        # Create a temporary file to serve the content
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w', encoding='utf-8') as temp_file:
            temp_file.write(content)
            temp_path = temp_file.name
        
        # Use file:// protocol to read the local file
        file_url = f"file://{temp_path}"
        
        try:
            # Use the crawler to process the file URL
            result = await crawler.arun(
                url=file_url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=llm_strategy,
                    session_id=session_id,
                ),
            )
            
            # Clean up the temporary file
            os.unlink(temp_path)
            
            if result.success and result.extracted_content:
                try:
                    return json.loads(result.extracted_content)
                except json.JSONDecodeError:
                    print("Failed to parse JSON from extraction result.")
                    return []
            else:
                print(f"Error processing content: {result.error_message}")
                return []
        except Exception as e:
            # Clean up the temporary file in case of error
            os.unlink(temp_path)
            print(f"Error during processing: {str(e)}")
            return []
    
    print(f"Content too large ({estimated_tokens} tokens), splitting into chunks...")
    
    # Split content based on estimated size
    chunk_size = max_tokens * 4  # Convert tokens to approximate character count
    content_chunks = []
    
    # Common patterns for news items in XML format
    patterns = [
        r'<item>.*?</item>',
        r'<article>.*?</article>',
        r'<entry>.*?</entry>',
        r'<news>.*?</news>',
        r'<record>.*?</record>',
        r'<channel>.*?</channel>',
    ]
    
    items = []
    for pattern in patterns:
        items = re.findall(pattern, content, re.DOTALL)
        if items:
            print(f"Found {len(items)} items using pattern '{pattern}'")
            break
    
    if items:
        # Group items into chunks that stay below token limit
        current_chunk = ""
        for item in items:
            if len(current_chunk + item) // 4 > max_tokens:
                if current_chunk:  # Only add non-empty chunks
                    content_chunks.append(current_chunk)
                current_chunk = item
            else:
                current_chunk += item
                
        if current_chunk:  # Add the last chunk
            content_chunks.append(current_chunk)
    else:
        # Fallback to simple character-based splitting
        print("No suitable XML patterns found, using size-based chunking...")
        for i in range(0, len(content), chunk_size):
            content_chunks.append(content[i:i+chunk_size])
    
    print(f"Split content into {len(content_chunks)} chunks")
    
    # Process each chunk
    all_items = []
    for i, chunk in enumerate(content_chunks):
        print(f"Processing chunk {i+1}/{len(content_chunks)}...")
        
        # Create a temporary file for this chunk
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False, mode='w', encoding='utf-8') as temp_file:
            # Make sure chunk is properly wrapped
            if not chunk.strip().startswith('<?xml') and llm_strategy.input_format == 'xml':
                temp_file.write(f'<?xml version="1.0" encoding="UTF-8"?><root>{chunk}</root>')
            else:
                temp_file.write(chunk)
            temp_path = temp_file.name
        
        # Use file:// protocol to read the local file
        file_url = f"file://{temp_path}"
        
        try:
            # Use the crawler to process the file URL
            result = await crawler.arun(
                url=file_url,
                config=CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=llm_strategy,
                    session_id=f"{session_id}_chunk_{i}",
                ),
            )
            
            # Clean up the temporary file
            os.unlink(temp_path)
            
            if result.success and result.extracted_content:
                try:
                    items = json.loads(result.extracted_content)
                    if isinstance(items, list):
                        all_items.extend(items)
                        print(f"Extracted {len(items)} items from chunk {i+1}.")
                    else:
                        all_items.append(items)
                        print(f"Extracted 1 item from chunk {i+1}.")
                except json.JSONDecodeError:
                    print(f"Failed to parse JSON from chunk {i+1}.")
            else:
                print(f"Error processing chunk {i+1}: {result.error_message}")
        except Exception as e:
            # Clean up the temporary file in case of error
            os.unlink(temp_path)
            print(f"Error during chunk {i+1} processing: {str(e)}")
        
        # Add delay between chunks to avoid rate limits
        await asyncio.sleep(1)
    
    return all_items


async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    base_url: str,
    css_selector: str,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    required_keys: List[str],
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """
    Fetches and processes a single page of news data with token limit handling.
    """
    url = f"{base_url}?page={page_number}"
    print(f"Loading page {page_number}...")

    # Check if "No Results Found" message is present
    no_results = await check_no_results(crawler, url, session_id)
    if no_results:
        return [], True  # No more results, signal to stop crawling

    # First, fetch the page content without extraction
    result = await crawler.arun(
        url=url,
        config=CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            session_id=session_id,
        ),
    )

    if not result.success:
        print(f"Error fetching page {page_number}: {result.error_message}")
        return [], False

    # Get the content to process
    content_to_process = result.cleaned_html
    
    # Apply CSS selector if provided
    if css_selector:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(content_to_process, 'html.parser')
        selected_elements = soup.select(css_selector)
        if not selected_elements:
            print(f"No content matching selector '{css_selector}' found on page {page_number}.")
            return [], False
        content_to_process = "".join(str(tag) for tag in selected_elements)

    # Try normal extraction first
    try:
        # Use our token-aware processing function
        from src.crawler.utils.scraper_utils import process_with_token_limit
        extracted_data = await process_with_token_limit(
            crawler,
            content_to_process,
            llm_strategy,
            f"{session_id}_page_{page_number}",
        )
    except Exception as e:
        print(f"Error processing content: {str(e)}")
        return [], False

    if not extracted_data:
        print(f"No news found on page {page_number}.")
        return [], False

    # Process the extracted data
    complete_newss = []
    for news in extracted_data:
        print("Processing news:", news)

        # Ignore the 'error' key if it's False
        if news.get("error") is False:
            news.pop("error", None)

        if not is_complete_news(news, required_keys):
            continue  # Skip incomplete newss

        if is_duplicate_news(news["title"], seen_names):
            print(f"Duplicate news '{news['title']}' found. Skipping.")
            continue  # Skip duplicate newss

        # Add news to the list
        seen_names.add(news["title"])
        complete_newss.append(news)

    if not complete_newss:
        print(f"No complete newss found on page {page_number}.")
        return [], False

    print(f"Extracted {len(complete_newss)} newss from page {page_number}.")
    return complete_newss, False  # Continue crawling