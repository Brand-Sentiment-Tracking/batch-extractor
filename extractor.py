import os
#import s3
import json
import logging
import re

import dotenv
dotenv.load_dotenv()

from dateutil.parser import parse as parse_date
from newsplease.crawler import commoncrawl_crawler as cc

WARC_DIRECTORY = os.environ.get("WARC_DIRECTORY")
ARTICLE_DIRECTORY = os.environ.get("ARTICLE_DIRECTORY")

VALID_HOST = os.environ.get("VALID_HOST")

CRAWL_START_DATE = os.environ.get("CRAWL_START_DATE")
CRAWL_END_DATE = os.environ.get("CRAWL_END_DATE")

S3_BUCKET_ADDRESS = os.environ.get("S3_BUCKET_ADDRESS")
S3_PRIVATE_KEY = os.environ.get("S3_PRIVATE_KEY")


def post_to_bucket(article):
    pass

def article_callback(article):
    article_name = re.sub(r"[^\w\.]+", "_", article.url)
    article_filepath = os.path.join(ARTICLE_DIRECTORY, f"{article_name}.json")
    
    post_to_bucket(article)

    with open(article_filepath, 'w') as article_fp:
        json.dump(article, article_fp, indent=4)

def warc_callback(*args):
    pass

if __name__ == "__main__":

    cc.crawl_from_commoncrawl(
        valid_hosts=[VALID_HOST],
        warc_files_start_date=parse_date(CRAWL_START_DATE),
        warc_files_end_date=parse_date(CRAWL_END_DATE),
        #start_date=parse_date(CRAWL_START_DATE),
        #end_date=parse_date(CRAWL_END_DATE),
        callback_on_article_extracted=article_callback,
        callback_on_warc_completed=warc_callback,
        local_download_dir_warc=WARC_DIRECTORY,
        number_of_extraction_processes=1,
        log_level=logging.INFO)