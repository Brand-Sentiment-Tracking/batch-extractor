import unittest

from .. import ExtractionJob


class TestExtractionJob(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.job = ExtractionJob()