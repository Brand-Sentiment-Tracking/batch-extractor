import json
import logging

from typing import Dict
from os import environ

from datetime import datetime, timedelta

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, types

from collections import OrderedDict

from loader import CCNewsArticleLoader
from newspaper import Article


class ArticleExtractor:

    def __init__(self, bucket_name, parquet_filepath, batch_upload_size):
        self.bucket_name = bucket_name
        self.parquet_filepath = parquet_filepath
        self.batch_upload_size = batch_upload_size

        self.loader = CCNewsArticleLoader(self.add_article)

        self.spark = SparkSession.builder \
            .appName("ArticleToParquet") \
                .getOrCreate()
        
        self.context = SparkContext.getOrCreate(SparkConf())

        self.schema = types.StructType([
            types.StructField('title', types.StringType(), True),
            types.StructField('main_text', types.StringType(), True),
            types.StructField('url', types.StringType(), False),
            types.StructField('source_domain', types.StringType(), False),
            types.StructField('date_publish', types.StringType(), True),
            types.StructField('date_crawled', types.StringType(), False),
            types.StructField('language', types.StringType(), False)
        ])

        self.articles = list()

    @staticmethod    
    def dict_to_row(article_dict: OrderedDict) -> Row:
        return Row(**article_dict)

    def add_article(self, article: Article, date_crawled: datetime,
                    counters: Dict[str, int]):
        
        date_published = article.publish_date.isoformat() \
            if article.publish_date is not None else None

        self.articles.append(OrderedDict([
            ("title", article.title),
            ("main_text", article.text),
            ("url", article.url),
            ("source_domain", article.source_url),
            ("date_publish", date_published),
            ("date_crawled", date_crawled.isoformat()),
            ("language", article.config.get_language())
        ]))

        if counters["extracted"] % self.batch_upload_size == 0:
            self.upload_to_parquet()
            self.articles = list()
        
    def upload_to_parquet(self):
        rows = self.context \
            .parallelize(self.articles) \
                .map(self.dict_to_row)

        articles_df = self.spark.createDataFrame(rows, self.schema)
        
        articles_df.write.mode('append') \
            .partitionBy("date_crawled", "language") \
            .parquet(f"s3a://{self.bucket_name}/{self.parquet_filepath}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    bucket_name = environ.get("S3_BUCKET_NAME")
    parquet_filepath = environ.get("PARQUET_FILEPATH")
    batch_upload_size = int(environ.get("BATCH_UPLOAD_SIZE"))

    url_patterns = json.loads(environ.get("URL_PATTERNS"))
    
    extractor = ArticleExtractor(bucket_name, parquet_filepath,
                                 batch_upload_size)
            
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1)

    logging.info(f"Downloading articles crawled between "
                f"{start_date.date()} and {end_date.date()}.")

    extractor.loader.download_articles(url_patterns, start_date, end_date)
