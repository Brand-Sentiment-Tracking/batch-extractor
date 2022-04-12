import re
import gzip
import requests
import logging
import langdetect

import os.path

from typing import Callable, Dict

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from fnmatch import fnmatch

from urllib3.response import HTTPResponse
from urllib.parse import urljoin

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newspaper import Article
from newspaper.utils import get_available_languages


class CommonCrawlArticleExtractor:
    """Load and parse articles from CommonCrawl News Archive.

    Note:
        This is for extracting articles from the CC-NEWS collection. For the
        CC-MAIN collections, use the `CCMainRecordLoader` class.

    Args:
        article_callback (callable): A function that is called once an article
            has been extracted.
    """
    WARC_PATHS = "warc.paths.gz"
    CC_DOMAIN = "https://data.commoncrawl.org"
    CC_NEWS_ROUTE = os.path.join("crawl-data", "CC-NEWS")

    WARC_FILE_RE = re.compile(r"CC-NEWS-(?P<time>\d{14})-(?P<serial>\d{5})")
    CONTENT_RE = re.compile(r"^(?P<mime>[\w\/]+);\s?charset=(?P<charset>.*)$")

    SUPPORTED_LANGUAGES = get_available_languages()

    ArticleCallback = Callable[[Article, datetime, Dict[str, int]], None]

    def __init__(self, article_callback=None):
        # Set article_callback to the empty callback if no function is passed
        self.article_callback = article_callback \
            if article_callback is not None \
            else self.__empty_callback

        self.patterns = list()

        self.__start_date = None
        self.__end_date = None

        self.reset_counters()

    @property
    def article_callback(self) -> ArticleCallback:
        """`callable`: Called once an article has been extracted.

        Note:
            The callback is passed one argument: the article as a
                `newspaper.Article` object.

        The setter method will throw a ValueError if the new callback is not
        a function.
        """
        return self.__article_callback

    @article_callback.setter
    def article_callback(self, func: ArticleCallback):
        if not callable(func):
            raise ValueError("Article callback is not a function.")

        self.__article_callback = func

    def __empty_callback(article: Article, date_crawled: datetime,
                         counters: Dict[str, int]):
        """Default function when an article_callback isn't specified.

        Note:
            This function does nothing.

        Args:
            article (Article): The extracted article.
        """
        return

    @property
    def patterns(self) -> "list[str]":
        """`list` of `str` containing the url patterns to match the
        article URL against when filtering.

        The setter method will throw a ValueError if the new patterns is not a
        list of strings.
        """
        return self.__patterns

    @patterns.setter
    def patterns(self, patterns: "list[str]"):
        if type(patterns) != list:
            raise ValueError("URL patterns is not a list.")
        elif any(map(lambda x: type(x) != str, patterns)):
            raise ValueError("Not all URL patterns are strings.")

        self.__patterns = patterns

    @property
    def start_date(self) -> datetime:
        """`datetime`: The starting date to filter the articles between.

        The setter method will throw a ValueError if the new date is not a
        `datetime` object or it is later than the end date.
        """
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date: datetime):
        if type(start_date) != datetime:
            raise ValueError("Start date isn't type 'datetime'.")
        elif start_date >= self.end_date:
            raise ValueError("Start date is on or after the end date.")

        self.__start_date = start_date

    @property
    def end_date(self) -> datetime:
        """`datetime`: The ending date to filter the articles between.

        The setter method will throw a ValueError if the new date is not a
        `datetime` object or it is in the future.
        """
        return self.__end_date

    @end_date.setter
    def end_date(self, end_date: datetime):
        if type(end_date) != datetime:
            raise ValueError("End date isn't type 'datetime'.")
        elif end_date >= datetime.now():
            raise ValueError("End date is in the future.")

        self.__end_date = end_date

    @property
    def extracted(self) -> int:
        """`int`: The number of articles successfully extracted."""
        return self.__extracted

    @property
    def discarded(self) -> int:
        """`int`: The number of articles discarded before extraction."""
        return self.__discarded

    @property
    def errored(self) -> int:
        """`int`: The number of articles that errored during extraction."""
        return self.__errored

    @property
    def counters(self) -> Dict[str, int]:
        """Return a dictionary of extracted/discarded/errored counters."""
        return {
            "extracted": self.extracted,
            "discarded": self.discarded,
            "errored": self.errored
        }

    def reset_counters(self):
        """Reset the counters for extracted/discarded/errored to zero."""
        self.__extracted = 0
        self.__discarded = 0
        self.__errored = 0

    def __load_warc_paths(self, month: int, year: int) -> "list[str]":
        """Returns a list of warc files for a single month/year archive.

        Note:
            If the files for a given month/year cannot be obtained, an empty
            list is returned.

        Args:
            month (int): The month to index (between 1 and 12).
            year (int): The year to index. Must be 4 digits.

        Returns:
            list[str]: A list of warc files in the archive for records
                crawled in the month and year passed.
        """
        paths_route = os.path.join(self.CC_NEWS_ROUTE, str(year),
                                   str(month).zfill(2), self.WARC_PATHS)

        paths_url = urljoin(self.CC_DOMAIN, paths_route)

        response = requests.get(paths_url)

        if response.ok:
            content = gzip.decompress(response.content)
            filenames = content.decode("utf-8").splitlines()
        else:
            logging.warn(f"Failed to download paths from '{paths_url}' "
                         f"(status code {response.status_code}).")

            filenames = list()

        return filenames

    def __extract_date(self, warc_filepath: str) -> datetime:
        match = self.WARC_FILE_RE.search(warc_filepath)

        if match is None:
            logging.debug(f"Ignoring '{warc_filepath}'.")
            return False

        time = match.group("time")

        return datetime.strptime(time, "%Y%m%d%H%M%S")

    def __is_within_date(self, warc_filepath: str) -> bool:
        """Checks whether a warc was crawled between the start and end dates.

        This is done by extracting the timetamp from the filename, parsing
        it to a datetime and comparing it to start_date and end_date.

        Note:
            If the filepath doesn't match the warc filename regex, the method
                will return False.

        Args:
            warc_filepath (str): The path from CC-NEWS domain to the file.
                The path is not checked, but the filename should have the
                following structure:
                    `CC-NEWS-20220401000546-00192.warc.gz`

        Returns:
            bool: True if the warc file was crawled within the start and end
                dates. False otherwise.
        """
        crawl_date = self.__extract_date(warc_filepath)

        return crawl_date >= self.start_date \
            and crawl_date < self.end_date

    def __filter_warc_paths(self, filepaths: "list[str]") -> "list[str]":
        """Filters the list of warc filepaths to those crawled between the
        start and end dates.

        Note:
            Any filepath that doesn't match the warc filename regex is
                automatically discarded.

        Args:
            filenames (list[str]): List of warc filepaths to filter.

        Returns:
            list[str]: The filtered list of warc filepaths.
        """
        return list(filter(self.__is_within_date, filepaths))

    def __retrieve_warc_paths(self) -> "list[str]":
        """Returns a list of warc filepaths from CC-NEWS that were crawled
        between the start and end dates.

        This done by looping through each monthly archive and extracting the
        ones that fall between the dates based on the timestamp within the
        warc filename.

        Returns:
            list[str]: A list of warc filepaths.
        """
        filenames = list()

        for d in rrule(MONTHLY, self.start_date, until=self.end_date):
            logging.info(f"Downloading warc paths for {d.strftime('%b %Y')}.")
            filenames.extend(self.__load_warc_paths(d.month, d.year))

        return self.__filter_warc_paths(filenames)

    def __is_valid_record(self, record: ArcWarcRecord) -> bool:
        """Checks whether a warc record should be extracted to an article.

        This is done by checking:
        - The record type is a response.
        - Its MIME type is `text/html` and its charset is UTF-8.
        - The source URL matches one of the url patterns.

        Args:
            record (ArcWarcRecord): The record to evaluate.

        Returns:
            bool: True if the record is valid and should be extracted to an
                article. False otherwise.
        """
        if record.rec_type != "response":
            return False

        source_url = record.rec_headers.get_header("WARC-Target-URI")
        content_string = record.http_headers.get_header('Content-Type')

        if source_url is None or content_string is None:
            return False

        content = self.CONTENT_RE.match(content_string)

        if content is None or content.group("mime") != "text/html" \
                or content.group("charset").lower() != "utf-8":

            return False

        return any(map(lambda url: fnmatch(source_url, url), self.patterns))

    def extract_article(self, url: str, html: str, language: str,
                        date_crawled: datetime):
        """Extracts the article from its html and update counters.

        Once successfully extracted, it is then passed to `article_callback`.

        Note:
            If the extraction process fails, the article will be discarded.

        Args:
            url (str): The source URL of the article.
            html (str): The complete HTML structure of the record.
            language (str): The two-char language code of the record.
        """
        if language not in self.SUPPORTED_LANGUAGES:
            logging.debug(f"Language not supported for '{url}'")
            self.__discarded += 1
            return

        article = Article(url, language=language)

        try:
            article.download(input_html=html)
            article.parse()
            self.__extracted += 1
        # Blanket error catch here. Should be made more specific
        except Exception as e:
            logging.warn(str(e))
            self.__errored += 1

        # Conditional here so exceptions in the callback are still raised
        if article.is_parsed:
            self.article_callback(article, date_crawled, self.counters)

    def __parse_records(self, warc: HTTPResponse, date_crawled: datetime):
        """Iterate through articles from a warc file.

        Each record is loaded using warcio, and extracted if:
        - It is a valid news article (see __is_valid_record)
        - Its source URL matches one of the patterns.
        - The detected language is supported by newspaper.

        Args:
            warc (HTTPResponse): The complete warc file as a stream.
        """
        for record in ArchiveIterator(warc, arc2warc=True):
            url = record.rec_headers.get_header("WARC-Target-URI")

            if not self.__is_valid_record(record):
                logging.debug(f"Ignoring '{url}'")
                self.__discarded += 1
                continue

            try:
                html = record.content_stream().read().decode("utf-8")
                language = langdetect.detect(html)
            except Exception:
                logging.debug(f"Couldn't decode '{url}'")
                self.__errored += 1
                continue

            self.extract_article(url, html, language, date_crawled)

    def __load_warc(self, warc_path: str):
        """Downloads and parses a warc file for article extraction.

        Note:
            If the response returns a bad status code, the method will exit
            without parsing the warc file.

        Args:
            warc_path (str): The route of the warc file to be downloaded (not
                including the CommonCrawl domain).
        """
        warc_url = urljoin(self.CC_DOMAIN, warc_path)
        date_crawled = self.__extract_date(warc_path)

        logging.info(f"Downloading '{warc_url}'")
        response = requests.get(warc_url, stream=True)

        if response.ok:
            self.__parse_records(response.raw, date_crawled)
        else:
            logging.warn(f"Failed to download warc from '{warc_url}' "
                         f"(status code {response.status_code}).")

    def download_articles(self, patterns: "list[str]", start_date: datetime,
                          end_date: datetime):
        """Downloads and extracts articles from CC-NEWS.

        Articles are extracted only if:
        - The source URL matches one of the URL patterns.
        - The article was crawled between the start and end dates.

        Args:
            patterns (list[str]): List of URL patterns the article must match.
            start_date (datetime): The earliest date the article must have
                been crawled.
            end_date (datetime): The latest date the article must have been
                crawled by.
        """
        self.patterns = patterns
        self.end_date = end_date
        self.start_date = start_date

        warc_paths = self.__retrieve_warc_paths()

        for warc in warc_paths:
            self.__load_warc(warc)
