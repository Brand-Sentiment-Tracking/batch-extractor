import json
import logging

from datetime import datetime, timedelta

from os import environ
from logging import INFO, DEBUG

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, types

from collections import OrderedDict

from loader import CCNewsArticleLoader
from newspaper import Article


ENVIRONMENT = environ.get("ENVIRONMENT_TYPE")
URL_PATTERNS = json.loads(environ.get("URL_PATTERNS"))
S3_BUCKET_NAME = environ.get("S3_BUCKET_NAME")

logging.basicConfig(level=DEBUG if ENVIRONMENT != "prod" else INFO)

spark = SparkSession.builder.appName("ArticleToParquet").getOrCreate()
context = SparkContext.getOrCreate(SparkConf())

schema = types.StructType([
    types.StructField('url', types.StringType(), False),
    types.StructField('date_publish', types.StringType(), True),
    types.StructField('title', types.StringType(), True),
    types.StructField('source_domain', types.StringType(), False),
    types.StructField('main_text', types.StringType(), True),
    types.StructField('date_crawled', types.StringType(), False),
    types.StructField('language', types.StringType(), False),
])

def dict_to_row(d):
    return Row(**OrderedDict(sorted(d.items())))

def article_callback(article: Article, date_crawled: datetime):
    date_published = article.publish_date.isoformat() \
        if article.publish_date is not None else None

    data = {
        "url": article.url,
        "date_publish": date_published,
        "title": article.title,
        "source_domain": article.source_url, # What is the difference between this and url?
        "maintext": article.text,
        "date_crawled": date_crawled.isoformat(),
        "language": article.config.get_language()
    }

    df = context.parallelize([data]) \
        .map(dict_to_row).toDF()

    df.write.mode('append') \
        .partitionBy("date_crawled", "language") \
            .parquet(f"{S3_BUCKET_NAME}.parquet")


end_date = datetime.today()
start_date = end_date - timedelta(days=5)

loader = CCNewsArticleLoader(article_callback)

logging.info(f"Downloading articles crawled between "
             f"{start_date.date()} and {end_date.date()}.")

loader.download_articles(URL_PATTERNS, start_date, end_date)
