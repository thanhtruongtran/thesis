import csv

from src.crawler.models.news import News


def is_duplicate_news(news_title: str, seen_titles: set) -> bool:
    return news_title in seen_titles


def is_complete_news(news: dict, required_keys: list) -> bool:
    return all(key in news for key in required_keys)


def save_newss_to_csv(newss: list, filename: str):
    if not newss:
        print("No newss to save.")
        return

    fieldnames = News.model_fields.keys()

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(newss)
    print(f"Saved {len(newss)} newss to '{filename}'.")
