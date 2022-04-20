import unittest

from ..extractor import ArticleExtractor


class TestCommonCrawlArticleExtractor(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.loader = ArticleExtractor()

    def test_update_valid_callback(self):
        pass

    def test_update_invalid_callback(self):
        pass

    def test_update_valid_url_patterns(self):
        pass

    def test_update_invalid_url_patterns(self):
        pass

    def test_update_valid_start_end_dates(self):
        pass

    def test_update_invalid_start_end_dates(self):
        pass

    def test_extracted_article_counter(self):
        pass

    def test_discarded_article_counter(self):
        pass

    def test_errored_article_counter(self):
        pass

    def test_reset_counts(self):
        pass

    def test_get_warc_paths_valid_date(self):
        pass

    def test_get_warc_paths_invalid_date(self):
        pass

    def test_extract_date_valid_warc_file(self):
        pass

    def test_extract_date_invalid_warc_file(self):
        pass

    def test_extract_valid_article(self):
        pass

    def test_extract_article_invalid_html(self):
        pass

    def test_extract_article_invalid_language(self):
        pass

    def test_dowload_article_integration_test(self):
        pass
