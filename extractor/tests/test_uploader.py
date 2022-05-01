import os
import unittest

from datetime import datetime

from .. import ArticleToParquetS3


class TestArticleToParquetS3(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = datetime.now()
        self.resources = "./extractor/tests/resources"

        self.bucket = "brand-sentiment-unit-testing/uploading"

        self.parquets = os.path.join(self.resources, "parquets")
        self.test_parquets = os.path.join(self.resources, "test-parquets")

        self.uploader = ArticleToParquetS3(self.bucket, 1000, processors=1,
                                           parquet_dir=self.parquets)
    
    def test_valid_bucket_name(self):
        pass

    def test_invalid_bucket_name(self):
        pass

    def test_valid_parquet_directory(self):
        self.uploader.parquet_dir = self.test_parquets

        self.assertEqual(self.uploader.parquet_dir, self.test_parquets)
        self.assertTrue(os.path.isdir(self.test_parquets))

    def test_invalid_parquet_directory(self):
        filepath = "./extractor/tests/resources/placeholder.txt"

        with self.assertRaises(ValueError) as a1:
            self.uploader.parquet_dir = 123
        
        with self.assertRaises(ValueError) as a2:
            self.uploader.parquet_dir = filepath

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Path is not a string.")
        self.assertEqual(e2, f"'{filepath}' is not a directory.")

    def test_valid_max_records(self):
        pass

    def test_invalid_max_records(self):
        pass

    def test_valid_partitions(self):
        pass

    def test_invalid_partitions(self):
        pass

    def test_upload_parquet_to_s3(self):
        pass

    def test_run_method(self):
        pass
