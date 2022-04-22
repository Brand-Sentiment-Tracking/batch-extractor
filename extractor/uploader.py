import os
import logging

from typing import List, Optional, Tuple

from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from . import ArticleExtractor


class ArticleToParquetS3:
    """Upload extracted articles from CC-NEWS to Amazon S3 as Parquet files.

    Note:
        Credentials for AWS are assumed to be held as environment variables.
        Make sure they are provided either by exporting `AWS_ACCESS_KEY_ID`
        and `AWS_SECRET_ACCESS_KEY`.

    Args:
        bucket (str): The name of the bucket on S3 to push to.
        parquet_dir (str): The filepath from the S3 bucket root to the
            Parquet file to push to.
        batch_size (int): The number of articles to extract before pushing as
            a batch.
        partitions (Tuple[str]): The set of keys to partition the parquet file
            by. All available keys can be found in `self.FIELDS`.
    """
    FIELDS = ("title", "main_text", "url", "source_domain",
              "date_publish", "date_crawled", "language")

    def __init__(self, bucket: str, max_records: int,
                 partitions: Optional[Tuple[str]] = None,
                 log_level: int = logging.INFO,
                 parquet_dir: str = "./parquets",
                 cores: int = None):

        self.logger = logging.getLogger("ArticleToParquetS3")
        self.logger.setLevel(log_level)

        self.bucket = bucket
        self.parquet_dir = parquet_dir

        self.max_records = max_records

        self.partitions = partitions if partitions is not None \
            else ("date_crawled", "language")

        self.extractor = ArticleExtractor(log_level, parquet_dir, cores)

        self.spark = SparkSession.builder \
            .appName("ArticleToParquet") \
            .getOrCreate()

        self.context = SparkContext.getOrCreate(SparkConf())

    @property
    def bucket(self) -> str:
        """`str`: The S3 bucket name to push the parquet files."""
        return self.__bucket

    @bucket.setter
    def bucket(self, name: str):
        if type(name) != str:
            raise ValueError("Bucket name is not a string.")

        self.__bucket = name

    @property
    def parquet_dir(self):
        return self.__parquet_dir

    @parquet_dir.setter
    def parquet_dir(self, path):
        if type(path) != str:
            raise ValueError("Path is not a string.")
        elif not os.path.exists(path):
            self.logger.debug(f"Creating directory '{path}'.")
            os.makedirs(path, exist_ok=True)
        elif not os.path.isdir(path):
            raise ValueError(f"'{path}' is not a directory.")

        self.__parquet_dir = path

    @property
    def parquet_url(self) -> str:
        """`str`: The Amazon S3 URL to the parquet file to push to."""
        return f"s3a://{self.bucket}/"

    @property
    def max_records(self) -> int:
        return self.__max_records

    @max_records.setter
    def max_records(self, n: int):
        if type(n) != int:
            raise ValueError("Max records is not an integer.")
        elif n <= 0:
            raise ValueError("Max records must be greater than 0.")

        self.__max_records = n

    @property
    def partitions(self) -> Tuple[str]:
        """`Tuple[str]`: The keys to partition the parquet file by in S3.

        The setter will raise a ValueError if the new keys are not a tuple of
        strings or one of the keys doesn't exist in the dataframe.
        """
        return self.__partitions

    @partitions.setter
    def partitions(self, keys: Tuple[str]):
        if type(keys) != tuple or len(keys) == 0:
            raise ValueError("Partition keys is not a tuple or is empty.")
        elif any(map(lambda k: type(k) != str, keys)):
            raise ValueError("Not all keys are strings.")
        elif any(map(lambda k: k not in self.FIELDS, keys)):
            raise ValueError("One of the keys doesn't exist.")

        self.__partitions = keys

    def report_counters(self):
        self.extractor.report_counters()

    def upload_parquet_to_s3(self, parquet_file):
        basename = os.path.basename(parquet_file)
        df = self.spark.read.parquet(parquet_file)

        self.logger.info(f"Pushing '{basename}' to '{self.bucket}' bucket.")

        df.repartition(*self.partitions) \
            .write.mode('append') \
            .option("maxRecordsPerFile", self.max_records) \
            .partitionBy(*self.partitions) \
            .parquet(self.parquet_url)

        self.logger.info(f"'{basename}' push successful.")

    def run(self, patterns: List[str], start_date: datetime,
            end_date: datetime):

        self.extractor.download_articles(patterns, start_date, end_date)

        if not self.extractor.parquet_files:
            self.logger.error("No parquet files were found.")
            return

        for parquet_file in self.extractor.parquet_files:
            self.upload_parquet_to_s3(parquet_file)
