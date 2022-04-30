import unittest

from .. import ArticleExtractor


class TestArticleExtractor(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.loader = ArticleExtractor()