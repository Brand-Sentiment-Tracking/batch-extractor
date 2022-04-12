from collections import OrderedDict
from typing import Dict, List, Optional

from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row, types

from extractor.extractor import CommonCrawlArticleExtractor
from newspaper import Article


class ArticleToParquetS3:

    KEYS = ["title", "main_text", "url", "source_domain", "date_publish",
            "date_crawled", "language"]

    def __init__(self, bucket_name: str, parquet_file: str,
                 upload_batch_size: int,
                 partitions: Optional[List[str]] = None):

        self.__bucket_name = None
        self.__parquet_file = None

        self.bucket_name = bucket_name
        self.parquet_file = parquet_file
        self.upload_batch_size = upload_batch_size

        self.partitions = partitions if partitions is not None \
            else ("date_crawled", "language")

        self.extractor = CommonCrawlArticleExtractor(self.add_article)

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

    @property
    def bucket_name(self) -> str:
        return self.__bucket_name

    @bucket_name.setter
    def bucket_name(self, name: str):
        if type(name) != str:
            raise ValueError("Bucket name is not a string.")

        self.__bucket_name = name

        if self.parquet_file is not None:
            self.update_bucket_url()

    @property
    def parquet_file(self) -> str:
        return self.__parquet_file

    @parquet_file.setter
    def parquet_file(self, filename: str):
        if type(filename) != str:
            raise ValueError("Parquet filepath is not a string.")

        self.__parquet_file = filename

        if self.bucket_name is not None:
            self.update_bucket_url()

    @property
    def upload_batch_size(self) -> int:
        return self.__upload_batch_size

    @upload_batch_size.setter
    def upload_batch_size(self, size: int):
        if type(size) != int or size <= 0:
            raise ValueError("Size is not an integer greater than zero.")

        self.__upload_batch_size = size

    @property
    def bucket_url(self) -> str:
        return self.__bucket_url

    def update_bucket_url(self):
        self.__bucket_url = f"s3a://{self.bucket_name}/{self.parquet_file}"

    @property
    def partitions(self) -> List[str]:
        return self.__partitions

    @partitions.setter
    def partitions(self, keys: List[str]):
        if type(keys) != tuple:
            raise ValueError("Partition keys is not a tuple.")
        elif any(map(lambda k: type(k) != str, keys)):
            raise ValueError("Not all keys are strings.")
        elif any(map(lambda k: k not in self.KEYS, keys)):
            raise ValueError("One of the keys doesn't exist.")

        self.__partitions = keys

    @staticmethod
    def dict_to_row(article_dict: OrderedDict) -> Row:
        return Row(**article_dict)

    def add_article(self, article: Article, date_crawled: datetime,
                    counters: Dict[str, int]):

        date_published = article.publish_date.isoformat() \
            if article.publish_date is not None else None

        self.articles.append(
            OrderedDict([
                ("title", article.title),
                ("main_text", article.text),
                ("url", article.url),
                ("source_domain", article.source_url),
                ("date_publish", date_published),
                ("date_crawled", date_crawled.isoformat()),
                ("language", article.config.get_language())
            ])
        )

        if counters["extracted"] % self.upload_batch_size == 0:
            self.upload_to_parquet()
            self.articles = list()

    def upload_to_parquet(self):
        rows = self.context \
            .parallelize(self.articles) \
            .map(self.dict_to_row)

        articles_df = self.spark.createDataFrame(rows, self.schema)

        articles_df.write.mode('append') \
            .partitionBy(*self.partitions) \
            .parquet(self.bucket_url)

    def run(self, patterns: List[str], start_date: datetime,
            end_date: datetime):

        self.extractor.download_articles(patterns, start_date, end_date)

        self.upload_to_parquet()
        self.articles = list()
