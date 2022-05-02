import os
import unittest

from datetime import datetime

from .. import ArticleToParquetS3


class TestArticleToParquetS3(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = datetime.now()
        self.resources = "./extractor/tests/resources"

        self.bucket = "brand-sentiment-unit-testing/ext/uploading"

        self.parquets = f"{self.resources}/parquets"
        self.test_parquets = f"{self.resources}/test-parquets"

        self.uploader = None

    def setUp(self):
        self.uploader = ArticleToParquetS3(self.bucket, 1000, processors=1,
                                           parquet_dir=self.parquets)
        return super().setUp()

    def test_valid_bucket_name(self):
        self.uploader.bucket = "yet-another-bucket"
        self.assertEqual(self.uploader.bucket, "yet-another-bucket")

    def test_invalid_bucket_name(self):
        with self.assertRaises(TypeError) as a:
            self.uploader.bucket = 123

        e = str(a.exception)
        self.assertEqual(e, "Bucket name is not a string.")

    def test_valid_parquet_directory(self):
        self.uploader.parquet_dir = self.test_parquets

        self.assertEqual(self.uploader.parquet_dir, self.test_parquets)
        self.assertTrue(os.path.isdir(self.test_parquets))

    def test_invalid_parquet_directory(self):
        filepath = f"{self.resources}/placeholder.txt"

        with self.assertRaises(TypeError) as a1:
            self.uploader.parquet_dir = 123

        with self.assertRaises(ValueError) as a2:
            self.uploader.parquet_dir = filepath

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Path is not a string.")
        self.assertEqual(e2, f"'{filepath}' is not a directory.")

    def test_valid_max_records(self):
        self.uploader.max_records = 50
        self.assertEqual(self.uploader.max_records, 50)

    def test_invalid_max_records(self):
        with self.assertRaises(TypeError) as a1:
            self.uploader.max_records = "Not too many records pls"

        with self.assertRaises(ValueError) as a2:
            self.uploader.max_records = -1

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Max records is not an integer.")
        self.assertEqual(e2, "Max records must be greater than 0.")

    def test_valid_partitions(self):
        new_partitions = ("title", "source_domain")
        self.uploader.partitions = new_partitions

        self.assertEqual(self.uploader.partitions, new_partitions)

    def test_invalid_partitions_bad_types(self):
        with self.assertRaises(TypeError) as a1:
            self.uploader.partitions = "source_domain"

        with self.assertRaises(ValueError) as a2:
            self.uploader.partitions = (123, "title")

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Partition keys is not a tuple or is empty.")
        self.assertEqual(e2, "Not all keys are strings.")

    def test_invalid_partitions_missing_or_duplicate_keys(self):
        with self.assertRaises(ValueError) as a1:
            self.uploader.partitions = ("url", "elon bought twitter")

        with self.assertRaises(ValueError) as a2:
            self.uploader.partitions = ("title", "title", "date_crawled")

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "One of the keys doesn't exist.")
        self.assertEqual(e2, "Cannot have duplicate keys.")

    def test_upload_parquet_to_s3(self):
        parquet = f"{self.resources}/CC-NEWS-20210102084234-01434.parquet"
        self.uploader.upload_parquet_to_s3(parquet)

    def test_run_method(self):
        end_date = datetime(2021, 1, 2, 9, 0, 0)
        start_date = datetime(2021, 1, 2, 6, 0, 0)

        self.uploader.run(["*"], start_date, end_date, 25)


if __name__ == "__main__":
    unittest.main()
