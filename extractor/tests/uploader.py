import unittest

from ..uploader import ArticleToParquetS3

class TestArticleToParquetS3(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.uploader = ArticleToParquetS3()