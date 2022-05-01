import os
import unittest
import pandas as pd

from datetime import datetime, timedelta

from .. import ArticleExtractor


class TestArticleExtractor(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = datetime.now()
        self.resources = "./extractor/tests/resources"

        self.parquets = f"{self.resources}/parquets"
        self.test_parquets = f"{self.resources}/test-parquets"

        self.extractor = None
    
    def setUp(self):
        self.extractor = ArticleExtractor(self.parquets, 1)

        return super().setUp()

    def test_valid_parquet_directory(self):
        self.extractor.parquet_dir = self.test_parquets

        self.assertEqual(self.extractor.parquet_dir, self.test_parquets)
        self.assertTrue(os.path.isdir(self.test_parquets))

    def test_invalid_parquet_directory(self):
        filepath = f"{self.resources}/placeholder.txt"

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
        self.extractor.start_date = datetime(2022, 4, 12)
        self.assertEqual(self.extractor.start_date, datetime(2022, 4, 12))

    def test_invalid_start_date(self):
        with self.assertRaises(ValueError) as a1:
            self.extractor.start_date = "2022-04-12"

        self.extractor.end_date = datetime(2022, 4, 12)

        with self.assertRaises(ValueError) as a2:
            self.extractor.start_date = datetime.now()
        
        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "Start date isn't type 'datetime'.")
        self.assertEqual(e2, "Start date is on or after the end date.")

    def test_valid_end_date(self):
        self.extractor.end_date = datetime(2022, 4, 12)
        self.assertEqual(self.extractor.end_date, datetime(2022, 4, 12))

    def test_invalid_end_date(self):
        with self.assertRaises(ValueError) as a1:
            self.extractor.end_date = "2022-04-12"

        self.extractor.end_date = datetime(2022, 4, 12)

        with self.assertRaises(ValueError) as a2:
            self.extractor.end_date = datetime.now() + timedelta(days=1)
        
        e1 = str(a1.exception)
        e2 = str(a2.exception)

        self.assertEqual(e1, "End date isn't type 'datetime'.")
        self.assertEqual(e2, "End date is in the future.")

    def test_valid_warc_is_within_date(self):
        self.extractor.end_date = datetime(2022, 4, 12)
        self.extractor.start_date = datetime(2022, 4, 11)

        valid_warc = "CC-NEWS/2022/04/CC-NEWS-20220411060308-00000.warc.gz"
        self.assertTrue(self.extractor.warc_is_within_date(valid_warc))

    def test_warc_is_not_within_date(self):
        self.extractor.end_date = datetime(2022, 4, 12)
        self.extractor.start_date = datetime(2022, 4, 11)

        invalid_warc = "CC-NEWS/2021/01/CC-NEWS-20210102060308-00000.warc.gz"
        self.assertFalse(self.extractor.warc_is_within_date(invalid_warc))

    def test_unknown_warc_is_not_within_date(self):
        self.extractor.end_date = datetime(2022, 4, 12)
        self.extractor.start_date = datetime(2022, 4, 11)

        unknown_warc = "CC-NEWS/2021/01/CC-NEWS-xxxxxxxxxxxxxx-xxxxx.warc.gz"
        self.assertFalse(self.extractor.warc_is_within_date(unknown_warc))
        
    def test_retrieve_warc_paths(self):
        filepath = f"{self.resources}/cc-news-warc-paths-01-2021.txt"

        start = datetime(2021, 1, 1)
        end = datetime(2021, 2, 1)

        warc_paths = self.extractor.retrieve_warc_paths(start, end)

        with open(filepath, "r") as f:
            correct_warc_paths = f.read().splitlines()

        self.assertEqual(len(warc_paths), len(correct_warc_paths))

        for path in correct_warc_paths:
            self.assertIn(path, warc_paths)

    def test_download_articles(self):
        """
        Only two WARC files fit this time range:
            CC-NEWS-20210102060308-01433.warc.gz
            CC-NEWS-20210102084234-01434.warc.gz
        """
        end_date = datetime(2021, 1, 2, 9, 0, 0)
        start_date = datetime(2021, 1, 2, 6, 0, 0)

        self.extractor.download_articles(["*"], start_date, end_date, 50)
        
        parquet_files = self.extractor.parquet_files
        counters = self.extractor.counters

        self.assertEqual(len(parquet_files), 2)
        self.assertEqual(counters.get("total"), 100)

        df1 = pd.read_parquet(parquet_files[0])
        df2 = pd.read_parquet(parquet_files[1])

        total_rows = df1.shape[0] + df2.shape[0]

        self.assertEqual(counters.get("extracted"), total_rows)

if __name__ == "__main__":
    unittest.main()