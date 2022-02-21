import os
import boto3
import json
import logging
import re

from dateutil.parser import parse as parse_date
from newsplease.crawler import commoncrawl_crawler as cc

from botocore.exceptions import ClientError

ENVIRONMENT = os.environ.get("ENVIRONMENT_TYPE")

WARC_DIRECTORY = os.environ.get("WARC_DIRECTORY")
ARTICLE_DIRECTORY = os.environ.get("ARTICLE_DIRECTORY")

VALID_HOSTS = json.loads(os.environ.get("VALID_HOSTS"))

CRAWL_START_DATE = os.environ.get("CRAWL_START_DATE")
CRAWL_END_DATE = os.environ.get("CRAWL_END_DATE")

S3_BUCKET_ADDRESS = os.environ.get("S3_BUCKET_ADDRESS")
S3_PRIVATE_KEY = os.environ.get("S3_PRIVATE_KEY")

s3_client = boto3.client("s3")

def upload_to_bucket(article_filepath):
    article_filename = os.path.basename(article_filepath)

    try:
        s3_client.upload_file(article_filepath, S3_BUCKET_ADDRESS, article_filename)
    except ClientError as e:
        logging.error(e)

def article_callback(article):
    article_name = re.sub(r"[^\w\.]+", "_", article.url)
    article_filepath = os.path.join(ARTICLE_DIRECTORY, f"{article_name}.json")
    
    with open(article_filepath, 'w') as article_fp:
        json.dump(article.__dict__, article_fp, default=str,
                  sort_keys=True, indent=4, ensure_ascii=False)
    
    upload_to_bucket(article, article_name)

def warc_callback(*args):
    pass

if __name__ == "__main__":

    os.makedirs(WARC_DIRECTORY, exist_ok=True)
    os.makedirs(ARTICLE_DIRECTORY, exist_ok=True)

    continue_after_error = ENVIRONMENT.lower() == "prod"

    cc.crawl_from_commoncrawl(
        valid_hosts=VALID_HOSTS,
        warc_files_start_date=parse_date(CRAWL_START_DATE),
        warc_files_end_date=parse_date(CRAWL_END_DATE),
        callback_on_article_extracted=article_callback,
        callback_on_warc_completed=warc_callback,
        continue_after_error=continue_after_error,
        local_download_dir_warc=WARC_DIRECTORY,
        number_of_extraction_processes=1,
        log_level=logging.INFO)