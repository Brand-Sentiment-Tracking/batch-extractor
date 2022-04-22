import re
import time
import logging
import requests
import langdetect

import pandas as pd

import os.path
import lxml.html

from traceback import format_exc
from typing import List, Dict

from datetime import datetime

from urllib3.response import HTTPResponse
from fnmatch import fnmatch

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newspaper import Article
from newspaper.utils import get_available_languages


class ExtractionJob:

    CONTENT_RE = re.compile(r"^(?P<mime>[\w\/]+);\s?charset=(?P<charset>.*)$")
    LANGUAGE_RE = re.compile(r"^(?P<language>\w{2})(?:$|[\-_](?P<dialect>\w+)$)")
    
    SUPPORTED_LANGUAGES = get_available_languages()

    FIELDS = ("title", "main_text", "url", "source_domain",
              "date_publish", "date_crawled", "language")

    def __init__(self, warc_url: str, patterns: List[str],
                 date_crawled: datetime, warc_dir: str = "./parquets",
                 log_level: int = logging.INFO, report_every: int = 5000):

        self.warc_url = warc_url
        self.patterns = patterns
        self.date_crawled = date_crawled

        self.warc_dir = warc_dir

        self.report_every = report_every

        self.basename = os.path.basename(warc_url).split(".")[0]
        self.job_name = f"ExtractionJob({self.basename})"

        self.articles = list()

        self.logger = logging.getLogger(self.job_name)
        self.logger.setLevel(log_level)

        self.reset_counters()

    @property
    def patterns(self) -> List[str]:
        """`list` of `str` containing the url patterns to match the
        article URL against when filtering.

        The setter method will throw a ValueError if the new patterns is not a
        list of strings.
        """
        return self.__patterns

    @patterns.setter
    def patterns(self, patterns: List[str]):
        if type(patterns) != list:
            raise ValueError("URL patterns is not a list.")
        elif any(map(lambda x: type(x) != str, patterns)):
            raise ValueError("Not all URL patterns are strings.")

        self.__patterns = patterns

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
        total = self.extracted + self.discarded + self.errored
        return {
            "extracted": self.extracted,
            "discarded": self.discarded,
            "errored": self.errored,
            "total": total
        }

    def reset_counters(self):
        """Reset the counters for extracted/discarded/errored to zero."""
        self.__extracted = 0
        self.__discarded = 0
        self.__errored = 0

    def report_counters(self):
        """Report the extracted/discarded/errored/total counters."""
        message = "Counter Update"

        for name, counter in self.counters.items():
            message += f" {name}={counter}"

        self.logger.info(message)

    def report_progress(self, start_time, offset, file_size):
        elasped_time = time.time() - start_time

        percent = 100 * offset / file_size
        remaining = 100 - percent

        seconds_left = elasped_time * remaining / percent
        minutes_left = seconds_left / 60
        
        self.logger.info(f"Extraction {percent:.2f}% complete. "
                         f"~{minutes_left:.0f} mins left.")

    @property
    def filename(self):
        return os.path.join(self.warc_dir, f"{self.basename}.parquet")

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

    def add_article(self, article: Article):
        try:
            date_published = article.publish_date.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            date_published = None

        self.articles.append({
            "title": article.title,
            "main_text": article.text,
            "url": article.url,
            "source_domain": article.source_url,
            "date_publish": date_published,
            "date_crawled": self.date_crawled.strftime("%Y-%m-%d"),
            "language": article.config.get_language()
        })

        #self.logger.error(f"'{article.title}' ({article.config.get_language()})")

    def extract_article(self, url: str, html: str, language: str):
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
            self.logger.debug(f"Language not supported for '{url}'")
            self.__discarded += 1
            return

        article = Article(url, language=language)

        try:
            article.download(input_html=html)
            article.parse()
            self.__extracted += 1

        # Blanket error catch here. Should be made more specific.
        except Exception:
            self.logger.debug(f"Parser raised exception:\n{format_exc()}")
            self.__errored += 1
            return

        self.add_article(article)

    def detect_language(self, html):
        parser = lxml.html.fromstring(html)

        language_string = str(parser.get("lang"))
        match = self.LANGUAGE_RE.match(language_string)
        
        language = match.group("language") \
            if match is not None else None

        if language is None:
            text = parser.text_content().strip()
            language = langdetect.detect(text)

        return language

    def __parse_records(self, warc: HTTPResponse, file_size: int):
        """Iterate through articles from a warc file.

        Each record is loaded using warcio, and extracted if:
        - It is a valid news article (see __is_valid_record)
        - Its source URL matches one of the patterns.
        - The detected language is supported by newspaper.

        Args:
            warc (HTTPResponse): The complete warc file as a stream.
        """
        records = ArchiveIterator(warc, arc2warc=True)
        self.logger.info("Iterating through records.")

        start_time = time.time()

        for i, record in enumerate(records):

            if i != 0 and i % self.report_every == 0:
                self.report_counters()
                self.report_progress(start_time, records.offset, file_size)

            url = record.rec_headers.get_header("WARC-Target-URI")

            if not self.__is_valid_record(record):
                self.logger.debug(f"Ignoring '{url}'")
                self.__discarded += 1
                continue

            try:
                html = record.content_stream().read().decode("utf-8")
                language = self.detect_language(html)
            
            except Exception:
                self.logger.debug(f"Record raised exception:\n{format_exc()}")
                self.__errored += 1
                continue

            self.extract_article(url, html, language)

    def save_to_parquet(self):
        self.logger.info(f"Saving to '{self.filename}'")
        pd.DataFrame(self.articles).to_parquet(self.filename)

    def extract_warc(self):
        """Downloads and parses a warc file for article extraction.

        Note:
            If the response returns a bad status code, the method will exit
            without parsing the warc file.

        Args:
            warc_path (str): The route of the warc file to be downloaded (not
                including the CommonCrawl domain).
        """
        self.logger.info(f"Downloading WARC file.")
        response = requests.get(self.warc_url, stream=True)

        if response.ok:
            file_size = int(response.headers.get("Content-Length"))
            self.__parse_records(response.raw, file_size)
        else:
            self.logger.warn(f"Failed to download '{self.basename}' "
                             f"(status code {response.status_code}).")

        self.save_to_parquet()
