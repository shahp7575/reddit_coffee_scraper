import praw
import logging
import time
import argparse
from zoneinfo import ZoneInfo
from config import get_credentials
from utils import save_to_csv
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedditScraper:
    def __init__(self):
        credentials = get_credentials()
        self.reddit = praw.Reddit(
            client_id=credentials["client_id"],
            client_secret=credentials["client_secret"],
            user_agent=credentials["user_agent"]
        )
        self.keyword="coffee"
        self.max_attempts=5

    def get_unix_timestamps_two_hours(self):
        """Gets the start and end timestamps for the last two hours in UTC timezone."""
        current_time = datetime.now(timezone.utc)
        start_time = current_time - timedelta(hours=2)

        print(f"Start time: {start_time} \t\t Current time: {current_time}")
        print(f"Start of day: {start_time}, Start of day TS: {start_time.timestamp()}")
        print(f"End of day: {current_time}, End of day TS: {current_time.timestamp()}")

        return int(start_time.timestamp()), int(current_time.timestamp())

    def scrape_posts(self, target_date=None):
        """Scrape reddit posts for keyword and target_date timeframe."""
        start_timestamp, end_timestamp = self.get_unix_timestamps_two_hours()
        posts = []
        attempt = 0

        while attempt < self.max_attempts:
            try:
                submissions = self.reddit.subreddit("all").search(self.keyword, sort="new", limit=500)
                new_posts = []
            
                for submission in submissions:
                    # get est timezone
                    created_at_utc = datetime.fromtimestamp(submission.created_utc, tz=ZoneInfo('UTC'))
                    created_at_est = created_at_utc.astimezone(ZoneInfo('America/New_York')).timestamp()
                    # filter for our current day
                    if start_timestamp <= created_at_est <= end_timestamp:
                        post_data = {
                            "id": submission.id,
                            "url": submission.url,
                            "title": submission.title,
                            "text": submission.selftext,
                            "score": submission.score,
                            "created_utc": submission.created_utc,
                            "subreddit": submission.subreddit.display_name,
                            "num_comments": submission.num_comments,
                            "upvote_ratio": submission.upvote_ratio,
                            "over_18": submission.over_18,
                        }
                        new_posts.append(post_data)
                if new_posts:
                    logger.info(f"Collected {len(new_posts)} new posts, total: {len(posts)}")
                
                break

            except praw.exceptions.RedditAPIException as e:
                logger.warning(f"Reddit API Expection: {e}. Retrying after a delay...")
                attempt += 10
                # time.sleep(60)

            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                attempt += 10
                # time.sleep(60)
        return start_timestamp, end_timestamp, new_posts

    def run(self):
        logger.info("Starting reddit scraping process...")
        st, et, posts = self.scrape_posts()
        if posts:
            filename = f"reddit_coffee_posts_{str(st)}_{str(et)}"
            save_to_csv(posts, filename)
            logger.info(f"Scraping completed successfully with {len(posts)} posts.")
        else:
            logger.info(f"No new posts found for today.")


if __name__ == "__main__":
    scraper = RedditScraper()
    scraper.run()