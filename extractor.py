import json
import logging

from datetime import datetime, timedelta

from os import environ
from logging import INFO, DEBUG

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from loader import CCNewsArticleLoader
from newspaper import Article


ENVIRONMENT = environ.get("ENVIRONMENT_TYPE")

S3_BUCKET_NAME = environ.get("S3_BUCKET_NAME")
PARQUET_FILE = environ.get("PARQUET_FILE")

URL_PATTERNS = json.loads(environ.get("URL_PATTERNS"))

logging.basicConfig(level=DEBUG if ENVIRONMENT != "production" else INFO)

spark = SparkSession.builder.appName("ArticleToParquet").getOrCreate()
context = SparkContext.getOrCreate(SparkConf())

def article_callback(article: Article, date_crawled: datetime):

    date_published = article.publish_date.isoformat() \
        if article.publish_date is not None else None

    data = {
        "url": article.url,
        "date_publish": date_published,
        "title": article.title,
        "source_domain": article.source_url,
        "maintext": article.text,
        "date_crawled": date_crawled.isoformat(),
        "language": article.config.get_language()
    }

    spark_df = spark.read.json(context.parallelize([json.dumps(data)]))
    spark_df.write.mode('append') \
        .partitionBy("date_crawled", "language") \
        .parquet(f"s3a://{S3_BUCKET_NAME}/{PARQUET_FILE}")


end_date = datetime.today()
start_date = end_date - timedelta(days=1)

loader = CCNewsArticleLoader(article_callback)

logging.info(f"Downloading articles crawled between "
             f"{start_date.date()} and {end_date.date()}.")

loader.download_articles(URL_PATTERNS, start_date, end_date)
