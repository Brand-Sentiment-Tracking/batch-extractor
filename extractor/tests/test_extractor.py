import os
import unittest

from datetime import datetime

from .. import ArticleExtractor


class TestArticleExtractor(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = datetime.now()
        self.resources = "./extractor/tests/resources"

        self.parquets = os.path.join(self.resources, "parquets")
        self.test_parquets = os.path.join(self.resources, "test-parquets")

        self.extractor = None
    
    def setUp(self):
        self.extractor = ArticleExtractor(self.parquets, 1)

        return super().setUp()

    def test_valid_parquet_directory(self):
        self.extractor.parquet_dir = self.test_parquets

        self.assertEqual(self.extractor.parquet_dir, self.test_parquets)
        self.assertTrue(os.path.isdir(self.test_parquets))

    def test_invalid_parquet_directory(self):
        filepath = "./extractor/tests/resources/placeholder.txt"

        with self.assertRaises(ValueError) as a1:
            self.extractor.parquet_dir = 123
        
        with self.assertRaises(ValueError) as a2:
            self.extractor.parquet_dir = filepath

        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Path is not a string.")
        self.assertEqual(e2, f"'{filepath}' is not a directory.")

    def test_valid_processors(self):
        self.extractor.processors = 4
        self.assertEqual(self.extractor.processors, 4)

        self.extractor.processors = None
        self.assertEqual(self.extractor.processors, os.cpu_count())

    def test_invalid_processors(self):
        with self.assertRaises(ValueError) as a1:
            self.extractor.processors = "I would like some CPUs plz"
        
        with self.assertRaises(ValueError) as a2:
            self.extractor.processors = -1

        with self.assertRaises(ValueError) as a3:
            self.extractor.processors = os.cpu_count() + 1
        
        e1 = str(a1.exception)
        e2 = str(a2.exception)
        e3 = str(a3.exception)

        self.assertEqual(e1, "Processors is not an integer greater than 0.")
        self.assertEqual(e2, "Processors is not an integer greater than 0.")

        self.assertEqual(e3, f"{os.cpu_count() + 1} processors is greater "
                         "than the number of CPUs available.")

    def test_valid_start_date(self):
        pass

    def test_invalid_start_date(self):
        pass

    def test_valid_end_date(self):
        pass

    def test_invalid_end_date(self):
        pass

    def test_report_counters(self):
        pass

    def test_warc_is_within_date(self):
        pass

    def test_retrieve_warc_paths(self):
        pass

    def test_download_articles(self):
        pass