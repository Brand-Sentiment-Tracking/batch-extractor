import re
import time
import logging
import requests
import langdetect

import pyarrow as pa
from pyarrow import parquet

import os.path

from traceback import format_exc
from typing import List, Dict, Optional

from datetime import datetime

from urllib3.response import HTTPResponse
from fnmatch import fnmatch

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from newspaper import Article


class ExtractionJob:
    """Download and extract articles from a single WARC file.

    This is designed to run concurrently with minimal shared memory, so when
    all the articles have been extracted, they are then saved to a parquet
    file locally (in `parquet_dir`) instead of being passed back as a
    variable.

    Args:
        warc_url (str): The WARC file URL to extract aricles from.
        patterns (List[str]): The glob patterns for filtering articles
            based off the source URL.
        date_crawled (datetime): The publish date/time of the WARC file.
        log_level (logging._Level): The severity level of logs to be
            reported, e.g., `DEBUG`, `INFO`, `WARN`, etc.
        report_every (int): The number of records to iterate through before
            reporting the status of the extraction job.
    """
    CONTENT_RE = re.compile(r"^(?P<mime>[\w\/]+);\s?charset=(?P<charset>.*)$")
    
    FIELDS = ("title", "main_text", "url", "source_domain",
              "date_publish", "date_crawled", "language")

    def __init__(self, warc_url: str, patterns: List[str],
                 date_crawled: datetime, parquet_dir: str = "./parquets",
                 log_level: int = logging.INFO, report_every: int = 5000):

        self.warc_url = warc_url
        self.patterns = patterns
        self.date_crawled = date_crawled
        self.parquet_dir = parquet_dir
        self.report_every = report_every

        self.articles = list()

        self.logger = logging.getLogger(self.job_name)
        self.logger.setLevel(log_level)

        self.reset_counters()

    @property
    def warc_url(self) -> str:
        return self.__warc_url

    @warc_url.setter
    def warc_url(self, url: str):
        if type(url) != str:
            raise ValueError("WARC URL is not a string.")
        
        self.__warc_url = url
        self.__basename = os.path.basename(url).split(".")[0]

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
    def date_crawled(self):
        return self.__date_crawled

    def date_crawled(self, date: datetime):
        if type(date) != datetime:
            raise ValueError("Date is not a datetime object.")
        elif date > datetime.now():
            raise ValueError("Date is in the future.")
            
        self.__date_crawled = date

    @property
    def parquet_dir(self) -> str:
        """`str`: The path to the local directory for storing parquet files.

        When defining the path, the setter will automatically create it if it
        doesn't exist. The setter will also raise a ValueError if the new path
        is not a string, or if the path exists but is not a directory.
        """
        return self.__parquet_dir

    @parquet_dir.setter
    def parquet_dir(self, path: str):
        if type(path) != str:
            raise ValueError("Path is not a string.")
        elif not os.path.exists(path):
            self.logger.debug(f"Creating directory '{path}'.")
            os.makedirs(path, exist_ok=True)
        elif not os.path.isdir(path):
            raise ValueError(f"'{path}' is not a directory.")

        self.__parquet_dir = path

    @property
    def report_every(self) -> int:
        return self.__report_every

    @report_every.setter
    def report_every(self, n: int):
        if type(n) != int:
            raise ValueError("Report Every is not an integer.")
        elif n <= 0:
            raise ValueError("Report Every must be greater than zero.")

        self.__report_every = n

    @property
    def basename(self) -> str:
        return self.__basename

    @property
    def job_name(self) -> str:
        return f"ExtractionJob({self.basename})"

    @property
    def filepath(self) -> str:
        return os.path.join(self.parquet_dir, f"{self.basename}.parquet")

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

    def report_progress(self, start_time: int, offset: int, file_size: int):
        """Log the percentage of records processed in the WARC file.

        This is based off the `Content-Length` header in the request and
        comparing it to the byte the iterator is currently reading (i.e.
        `ArchiveIterator.offset`).

        Note:
            If Content-Length is None or zero, no report will be logged.

        Args:
            start_time (int): The UNIX timestamp of when the record
                iteration began. This is used to determine approximately how
                long it will take to complete the extraction job.
            offset (int): The byte position currently being read by the
                ArchiveIterator.
            file_size (int): The total number of bytes in the WARC file, given
                by the Content-Length header of the request.
        """
        if file_size is None or file_size == 0:
            self.logger.debug("Filesize unknown, cannot report progress.")
            return

        minutes = (time.time() - start_time) / 60

        percent_complete = 100 * offset / file_size
        percent_remaining = 100 - percent_complete

        minutes_left = minutes * percent_remaining / percent_complete
        
        self.logger.info(f"Extraction {percent_complete:.2f}% complete. "
                         f"~{minutes_left:.0f} mins left.")

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

    def add_article(self, article: Article, language: str):
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
            "language": language
        })

    def extract_article(self, url: str, html: str):
        """Extracts the article from its html and update counters.

        Once successfully extracted, it is then passed to `article_callback`.

        Note:
            If the extraction process fails, the article will be discarded.

        Args:
            url (str): The source URL of the article.
            html (str): The complete HTML structure of the record.
            language (str): The two-char language code of the record.
        """
        article = Article(url)

        try:
            article.download(input_html=html)
            article.parse()

            if article.text:
                language = langdetect.detect(article.text)
            else:
                language = langdetect.detect(article.title)

            self.__extracted += 1

        # Blanket error catch here. Should be made more specific.
        except Exception:
            self.logger.debug(f"Parser raised exception:\n{format_exc()}")
            self.__errored += 1

            return

        self.add_article(article, language)

    def __parse_records(self, warc: HTTPResponse, file_size: Optional[int]):
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

            if i > 200:
                self.logger.info("Early stop.")
                break

            if i != 0 and i % self.report_every == 0:
                self.report_progress(start_time, records.offset, file_size)

            url = record.rec_headers.get_header("WARC-Target-URI")

            if not self.__is_valid_record(record):
                self.logger.debug(f"Ignoring '{url}'")
                self.__discarded += 1
                continue

            try:
                html = record.content_stream().read().decode("utf-8")
            except Exception:
                self.logger.debug(f"Record raised exception:\n{format_exc()}")
                self.__errored += 1
                continue

            self.extract_article(url, html)

    def save_to_parquet(self):
        self.logger.info(f"Saving to '{self.filepath}'")
        table = pa.Table.from_pylist(self.articles)

        parquet.write_table(table, self.filepath, flavor="spark")

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
            file_size_string = response.headers.get("Content-Length")
            
            file_size = int(file_size_string) \
                if file_size_string is not None \
                else None

            self.__parse_records(response.raw, file_size)
        else:
            self.logger.warn(f"Failed to download '{self.basename}' "
                             f"(status code {response.status_code}).")

        self.save_to_parquet()
