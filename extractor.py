import os
import boto3
import json
import logging
import re

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

from datetime import datetime, date, timedelta
from newsplease.crawler import commoncrawl_crawler as cc
from botocore.exceptions import ClientError

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

def article_callback(article, spark, sc):
    name = re.sub(r"[^\w\.]+", "_", article.url)

    try:
        data = json.dumps(article.__dict__, ensure_ascii=False)
        spark_df = spark.read.json(sc.parallelize([data]))
        spark_df.write.mode('append').partitionBy("date_download","language").parquet(f"{S3_BUCKET_NAME}.parquet")
        
    except UnicodeEncodeError:
        logging.error(f"Failed to save {name} due to ascii encoding error.")

    # upload_to_bucket(filepath, s3_filename)

def warc_callback(*args):
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    os.makedirs(WARC_DIRECTORY, exist_ok=True)
    os.makedirs(ARTICLE_DIRECTORY, exist_ok=True)

    continue_after_error = ENVIRONMENT.lower() == "prod"

    today = date.today()
    today_date = datetime(today.year, today.month, today.day)

    end_date = today_date - timedelta(days=1)
    start_date = end_date - timedelta(days=1)

    logging.info(f"Downloading articles crawled between "
                    f"{start_date} and {end_date}.")

    #logging.info(f"Extracting from: \n{'\n\t'.join(VALID_HOSTS)}")
    logging.info(f"Continuing after error? {continue_after_error}")
    logging.info(f"warc directory: {WARC_DIRECTORY}")
    logging.info(f"Article directory: {ARTICLE_DIRECTORY}")

    spark = SparkSession.builder.appName("JsonToParquetPyspark").getOrCreate()
    sc = SparkContext.getOrCreate(SparkConf())

    cc.crawl_from_commoncrawl(
        valid_hosts=VALID_HOSTS,
        warc_files_start_date=start_date,
        warc_files_end_date=end_date,
        callback_on_article_extracted=article_callback,
        callback_on_warc_completed=warc_callback,
        continue_after_error=continue_after_error,
        local_download_dir_warc=WARC_DIRECTORY,
        number_of_extraction_processes=1,
        log_level=logging.INFO)