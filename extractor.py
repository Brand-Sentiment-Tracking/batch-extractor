import os
import boto3
import json
import logging
import re

from datetime import datetime, date, timedelta
from botocore.exceptions import ClientError

from loader import CCNewsArticleLoader


ENVIRONMENT = os.environ.get("ENVIRONMENT_TYPE")

WARC_DIRECTORY = os.environ.get("WARC_DIRECTORY")
ARTICLE_DIRECTORY = os.environ.get("ARTICLE_DIRECTORY")
VALID_HOSTS = json.loads(os.environ.get("VALID_HOSTS"))

S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

s3 = boto3.client("s3")


def upload_to_bucket(filepath, filename):    
    try:
        s3.upload_file(filepath, S3_BUCKET_NAME, filename)
    except ClientError as e:
        logging.error(e)

def article_callback(article):
    name = re.sub(r"[^\w\.]+", "_", article.url)
    subdirectory = date.today().isoformat()

    s3_filename = os.path.join(subdirectory, f"{name}.json")
    subdirectory_path = os.path.join(ARTICLE_DIRECTORY, subdirectory)
    filepath = os.path.join(ARTICLE_DIRECTORY, s3_filename)
    
    if not os.path.exists(subdirectory_path):
        os.makedirs(subdirectory_path)

    try:
        with open(filepath, 'w') as article_fp:
            json.dump(article.__dict__, article_fp, default=str,
                    sort_keys=True, indent=4, ensure_ascii=False)
    except UnicodeEncodeError:
        logging.error(f"Failed to save {name} due to ascii encoding error.")

    upload_to_bucket(filepath, s3_filename)


if __name__ == "__main__":
    log_level = logging.DEBUG if ENVIRONMENT != "prod" else logging.INFO

    logging.basicConfig(level=log_level)

    os.makedirs(WARC_DIRECTORY, exist_ok=True)
    os.makedirs(ARTICLE_DIRECTORY, exist_ok=True)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=5)

    loader = CCNewsArticleLoader(article_callback)

    logging.info(f"Downloading articles crawled between "
                 f"{start_date.date()} and {end_date.date()}.")
    
    loader.download_articles(VALID_HOSTS, start_date, end_date)