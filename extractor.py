import os
import boto3
import json
import logging
import re

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

from datetime import datetime, timedelta
from botocore.exceptions import ClientError

from loader import CCNewsArticleLoader


ENVIRONMENT = os.environ.get("ENVIRONMENT_TYPE")
WARC_DIRECTORY = os.environ.get("WARC_DIRECTORY")
ARTICLE_DIRECTORY = os.environ.get("ARTICLE_DIRECTORY")
VALID_HOSTS = json.loads(os.environ.get("VALID_HOSTS"))
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")

log_level = logging.DEBUG if ENVIRONMENT != "prod" else logging.INFO

logging.basicConfig(level=log_level)

os.makedirs(WARC_DIRECTORY, exist_ok=True)
os.makedirs(ARTICLE_DIRECTORY, exist_ok=True)

s3 = boto3.client("s3")

spark = SparkSession.builder.appName("JsonToParquetPyspark").getOrCreate()
sc = SparkContext.getOrCreate(SparkConf())


def upload_to_bucket(filepath, filename):    
    try:
        s3.upload_file(filepath, S3_BUCKET_NAME, filename)
    except ClientError as e:
        logging.error(e)

def article_callback(article):
    name = re.sub(r"[^\w\.]+", "_", article.url)

    try:
        data = json.dumps(article.__dict__, ensure_ascii=False)
        spark_df = spark.read.json(sc.parallelize([data]))
        spark_df.write.mode('append').partitionBy("date_download","language").parquet(f"{S3_BUCKET_NAME}.parquet")
        
    except UnicodeEncodeError:
        logging.error(f"Failed to save {name} due to ascii encoding error.")

    # upload_to_bucket(filepath, s3_filename)


end_date = datetime.today()
start_date = end_date - timedelta(days=5)

loader = CCNewsArticleLoader(article_callback)

logging.info(f"Downloading articles crawled between "
             f"{start_date.date()} and {end_date.date()}.")

loader.download_articles(VALID_HOSTS, start_date, end_date)
