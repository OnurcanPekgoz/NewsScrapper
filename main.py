import json
import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import matplotlib.pyplot as plt
import time

# MongoDB configurations
MONGO_URI = 'mongodb://localhost:27017'
DB_NAME = 'onurcan_pekgoz'

# Logging configuration
logging.basicConfig(filename='logs/logs.log', level=logging.INFO)


class NewsScraper:
    def __init__(self, main_page_url, mongo_uri, db_name):
        self.main_page_url = main_page_url
        self.mongo_uri = mongo_uri
        self.db_name = db_name

    # Function that fetches news details
    def fetch_news(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            header = soup.find('h1', {'class': 'single_title'}).text.strip()
            summary = soup.find('h2', {'class': 'single_excerpt'}).text.strip()

            text_div = soup.find('div', {'class': 'yazi_icerik'})
            paragraphs = text_div.find_all('p')
            text = ' '.join([p.text.strip() for p in paragraphs])

            img_tags = soup.find_all('img', {'class': 'rhd-article-news-img'})
            img_url_list = [img['data-src'] for img in img_tags]

            json_data = soup.find('script', {'class': 'rank-math-schema'}).text
            json_data = json.loads(json_data)
            publish_date = datetime.strptime(json_data['@graph'][5]['datePublished'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                '%Y-%m-%d')
            update_date = datetime.strptime(json_data['@graph'][5]['dateModified'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                '%Y-%m-%d')

            return {
                'url': url,
                'header': header,
                'summary': summary,
                'text': text,
                'img_url_list': img_url_list,
                'publish_date': publish_date,
                'update_date': update_date
            }

        except requests.RequestException as e:
            logging.error(f"Error making a request to {url}: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON data from {url}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}", exc_info=True)
            return None

    # Function that fetches pages and call fetch_news for all news
    def fetch_page(self, current_page_url):
        try:
            response = requests.get(current_page_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            news_links = [a['href'] for a in soup.find_all('a', {'class': 'post-link'})[10:20]]

            news = []
            for link in news_links:
                news_url = link
                news_data = self.fetch_news(news_url)
                if news_data:
                    news.append(news_data)

            return news

        except requests.RequestException as e:
            logging.error(f"Error making a request to {current_page_url}: {str(e)}")
            return []
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}", exc_info=True)
            return []

    # Function that finds most common 10 words across all news texts and store to database
    def find_most_common_words(self, news_texts):
        combined_text = ' '.join(news_texts)
        words = combined_text.split()
        word_counter = Counter(words)

        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            word_frequency_collection = db['word_frequency']

            word_frequency_collection.delete_many({})

            most_common_words = word_counter.most_common(10)

            for word, count in most_common_words:
                word_frequency_collection.insert_one({'word': word, 'count': count})
            logging.info("Successfully inserted word frequency data into the MongoDB 'word_frequency' collection.")

            self.plot_most_common_words(most_common_words)

        except Exception as e:
            logging.error(f"Error storing word frequency data in MongoDB: {str(e)}")

    # Function that plots most common 10 words across all news texts
    @staticmethod
    def plot_most_common_words(most_common_words):
        words, counts = zip(*most_common_words)
        plt.bar(words, counts)
        plt.xlabel('Words')
        plt.ylabel('Frequency')
        plt.title('Most Common Words')
        plt.savefig('graph.png')
        plt.show()

    # Function that stores news data to database
    def store_news_data(self, news):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            news_collection = db['news']
            news_collection.insert_many(news)
            logging.info("Successfully inserted news data into the MongoDB 'news' collection.")

        except Exception as e:
            logging.error(f"Error storing news data in MongoDB: {str(e)}")

    # Function that stores stats data to database
    def store_stats(self, elapsed_time, count, success_count, fail_count):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            stats_collection = db['stats']
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M')

            stats_data = {
                'elapsed_time': elapsed_time,
                'count': count,
                'date': current_date,
                'success_count': success_count,
                'fail_count': fail_count
            }

            stats_collection.insert_one(stats_data)
            logging.info("Successfully inserted stats data into the MongoDB 'stats' collection.")

        except Exception as e:
            logging.error(f"Error storing stats data in MongoDB: {str(e)}")

    # Function that analyzes stats and calls stor_stats
    def analyze_and_store_data(self, page_number):
        start_time = time.time()

        current_page_url = f"{self.main_page_url}/page/{page_number}/"

        news = self.fetch_page(current_page_url)

        end_time = time.time()
        elapsed_time = end_time - start_time
        count = len(news)
        success_count = len([item for item in news if item])
        fail_count = count - success_count

        self.store_stats(elapsed_time, count, success_count, fail_count)

        if not news:
            logging.warning(f"No news results to insert into MongoDB for page {page_number}.")
            return

        self.store_news_data(news)

    # Function that prints grouped news data
    def print_data_grouped_by_update_date(self):
        try:
            client = MongoClient(self.mongo_uri)
            db = client[self.db_name]
            news_collection = db['news']

            pipeline = [
                {"$group": {"_id": "$update_date", "count": {"$sum": 1},
                            "news": {"$push": {"header": "$header", "update_date": "$update_date"}}}},
                {"$sort": {"_id": 1}}
            ]

            grouped_data = list(news_collection.aggregate(pipeline))

            for group in grouped_data:
                print(f"\nUpdate Date: {group['_id']}, Count: {group['count']}")
                for news_item in group['news']:
                    print(f"Header: {news_item['header']}, Update Date: {news_item['update_date']}")

        except Exception as e:
            logging.error(f"Error printing data grouped by update_date: {str(e)}")


def main():
    scraper = NewsScraper(main_page_url='https://turkishnetworktimes.com/kategori/gundem/', mongo_uri=MONGO_URI,
                          db_name=DB_NAME)
    threads = 10
    max_page_numbers = 50
    with ThreadPoolExecutor(max_workers=threads) as executor:
        page_numbers = range(1, max_page_numbers + 1)
        executor.map(scraper.analyze_and_store_data, page_numbers)

    all_news_texts = []
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    news_collection = db['news']
    for item in news_collection.find():
        all_news_texts.append(item['text'])

    scraper.find_most_common_words(all_news_texts)
    scraper.print_data_grouped_by_update_date()


if __name__ == "__main__":
    main()
