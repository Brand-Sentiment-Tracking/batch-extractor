import os
import logging
import unittest
import pandas as pd

from datetime import datetime, timedelta
from warcio.archiveiterator import ArchiveIterator

from .. import ExtractionJob


class TestExtractionJob(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_date = datetime.now()
        self.resources = "./extractor/tests/resources"

        self.parquets = os.path.join(self.resources, "parquets")
        self.test_parquets = os.path.join(self.resources, "test-parquets")

    def setUp(self) -> None:
        self.job = ExtractionJob("test-warc", ["*"], self.start_date,
                                 self.parquets)
        
        return super().setUp()

    def test_valid_warc_url(self):
        url_string = "https://data.commoncrawl.org/crawl-data/" \
                     "CC-NEWS/2021/01/CC-NEWS-20210101235306-01431.warc.gz"
        
        basename = "CC-NEWS-20210101235306-01431"

        self.job.warc_url = url_string

        self.assertEqual(self.job.warc_url, url_string)
        self.assertEqual(self.job.basename, basename)
        self.assertEqual(self.job.job_name, f"ExtractionJob({basename})")

    def test_invalid_warc_url(self):
        random_args = [None, 1, True, list(), dict()]

        for arg in random_args:
            with self.assertRaises(ValueError) as a:
                self.job.warc_url = arg

            self.assertEqual(str(a.exception), "WARC URL is not a string.")

    def test_valid_url_patterns(self):
        example_patterns = ["*bbc.co.uk*", "*news.sky.com*"]

        self.job.patterns = example_patterns
        self.assertEqual(self.job.patterns, example_patterns)

    def test_invalid_url_patterns(self):
        with self.assertRaises(ValueError) as a1:
            self.job.patterns = "Hello World!"

        with self.assertRaises(ValueError) as a2:
            self.job.patterns = ["*bbc.co.uk*", list(), 1, "*news.sky.com*"]

        e1 = a1.exception
        e2 = a2.exception

        self.assertEqual(str(e1), "URL patterns is not a list.")
        self.assertEqual(str(e2), "Not all URL patterns are strings.")

    def test_valid_date_crawled(self):
        new_date = datetime.now() - timedelta(days=324)
        self.job.date_crawled = new_date

        self.assertEqual(self.job.date_crawled, new_date)

    def test_invalid_date_crawled(self):
        with self.assertRaises(ValueError) as a1:
            self.job.date_crawled = "Not a date"
        
        with self.assertRaises(ValueError) as a2:
            self.job.date_crawled = datetime.now() + timedelta(days=1)

        e1 = a1.exception
        e2 = a2.exception

        self.assertEqual(str(e1), "Date is not a datetime object.")
        self.assertEqual(str(e2), "Date is in the future.")

    def test_valid_parquet_directory(self):
        self.job.parquet_dir = self.test_parquets

        self.assertEqual(self.job.parquet_dir, self.test_parquets)
        self.assertTrue(os.path.isdir(self.test_parquets))

    def test_invalid_parquet_directory(self):
        filepath = "./extractor/tests/resources/placeholder.txt"

        with self.assertRaises(ValueError) as a1:
            self.job.parquet_dir = 123
        
        with self.assertRaises(ValueError) as a2:
            self.job.parquet_dir = filepath

        e1 = a1.exception
        e2 = a2.exception

        self.assertEqual(str(e1), "Path is not a string.")
        self.assertEqual(str(e2), f"'{filepath}' is not a directory.")

    def test_valid_report_every(self):
        self.job.report_every = 50
        self.assertEqual(self.job.report_every, 50)

    def test_invalid_report_every(self):
        with self.assertRaises(ValueError) as a1:
            self.job.report_every = "Not a number"
        
        with self.assertRaises(ValueError) as a2:
            self.job.report_every = 0
        
        e1 = a1.exception
        e2 = a2.exception

        self.assertEqual(str(e1), "Report Every is not an integer.")
        self.assertEqual(str(e2), "Report Every must be greater than zero.")

    """Testing ExtractionJob public methods."""

    def test_is_valid_record_not_response(self):
        file = "test-request.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))


    def test_is_valid_record_no_source_url(self):
        file = "test-invalid-response-no-source.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))

    def test_is_valid_record_no_content_type(self):
        file = "test-invalid-response-no-content-type.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))

    def test_is_valid_record_bad_encoding(self):
        file = "test-invalid-response-not-utf8.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))

    def test_is_valid_record_bad_mimetype(self):
        file = "test-invalid-response-not-text-html.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))

    def test_is_valid_record_no_matching_pattern(self):
        self.job.patterns = ["*bbc.co.uk*"]

        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertFalse(self.job.is_valid_record(record))

    def test_is_valid_record_with_valid_record(self):
        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            self.assertTrue(self.job.is_valid_record(record))

    def test_extract_article(self):
        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            html = record.content_stream().read().decode("utf-8")
        
        article, language = self.job.extract_article(path, html)

        self.assertIsNotNone(article)
        self.assertIsNotNone(language)

        self.assertIsNotNone(article.title)
        self.assertIsNotNone(article.text)
        self.assertIsNotNone(article.publish_date)
        self.assertIsNotNone(article.source_url)
        self.assertIsNotNone(article.url)

    def test_add_article_no_publish_date(self):
        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            html = record.content_stream().read().decode("utf-8")
        
        article, language = self.job.extract_article(path, html)
        
        article.publish_date = None
        self.job.add_article(article, language)

        self.assertIsNone(self.job.articles[0].get("date_publish"))

    def test_add_article_with_publish_date(self):
        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            html = record.content_stream().read().decode("utf-8")
        
        article, language = self.job.extract_article(path, html)
        self.job.add_article(article, language)

        self.assertIsNotNone(self.job.articles[0].get("date_publish"))

    def test_save_to_parquet(self):
        file = "test-valid-response.warc"
        path = os.path.join(self.resources, file)

        with open(path, "rb") as f:
            record = next(ArchiveIterator(f, arc2warc=True))
            html = record.content_stream().read().decode("utf-8")
        
        article, language = self.job.extract_article(path, html)
        self.job.add_article(article, language)

        self.assertIsNotNone(self.job.articles[0].get("date_publish"))

        self.job.save_to_parquet()

        filepath = f"{self.parquets}/{self.job.basename}.parquet"

        df = pd.read_parquet(filepath)

        publish_date = article.publish_date.strftime("%Y-%m-%d")
        crawled_date = self.job.date_crawled.strftime("%Y-%m-%d")

        self.assertEqual(df.title.values[0], article.title)
        self.assertEqual(df.main_text.values[0], article.text)
        self.assertEqual(df.url.values[0], article.url)
        self.assertEqual(df.source_domain.values[0], article.source_url)
        self.assertEqual(df.date_publish.values[0], publish_date)
        self.assertEqual(df.date_crawled.values[0], crawled_date)
        self.assertEqual(df.language.values[0], language)


    def test_extract_warc(self):
        url_string = "https://data.commoncrawl.org/crawl-data/" \
                     "CC-NEWS/2021/01/CC-NEWS-20210101235306-01431.warc.gz"

        self.job.warc_url = url_string

        self.job.extract_warc(limit=50)
        self.job.report_counters()

        self.assertGreater(self.job.extracted, 0)
        self.assertGreater(0.1 * self.job.extracted, self.job.errored)

        filepath = f"{self.parquets}/{self.job.basename}.parquet"

        self.assertTrue(os.path.isfile(filepath))
        df = pd.read_parquet(filepath)

        self.assertEqual(self.job.extracted, df.shape[0])


if __name__ == "__main__":
    unittest.main()