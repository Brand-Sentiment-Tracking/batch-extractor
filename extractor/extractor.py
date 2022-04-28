import os
import re
import gzip
import requests
import logging
import multiprocessing

from typing import List, Dict, Optional, Tuple

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from urllib.parse import urljoin

from multiprocessing.pool import Pool

from .extraction_job import ExtractionJob


class ArticleExtractor:
    """Load and parse articles from CommonCrawl News Archive.

    Args:
        log_level (logging._Level): The severity level of logs to be
            reported, e.g., `DEBUG`, `INFO`, `WARN`, etc.
        parquet_dir (str): The path to the local directory for storing
            parquet files after extraction. The directory will automatically
            be created if it doesn't already exist.
        processes (int): The number of parallel processes to run when
            extracting articles. If `None`, then the number of CPUs available
            is used.
    """
    WARC_PATHS = "warc.paths.gz"
    CC_DOMAIN = "https://data.commoncrawl.org"
    CC_NEWS_ROUTE = os.path.join("crawl-data", "CC-NEWS")

    WARC_FILE_RE = re.compile(r"CC-NEWS-(?P<time>\d{14})-(?P<serial>\d{5})")

    def __init__(self, log_level: int = logging.INFO,
                 parquet_dir: str = "./parquets",
                 processes: Optional[int] = None):

        self.log_level = log_level

        self.logger = logging.getLogger("ArticleExtractor")
        self.logger.setLevel(self.log_level)

        self.parquet_dir = parquet_dir
        self.processes = processes

        self.__start_date = None
        self.__end_date = None

        self.reset_counters()

        self.parquet_files = list()

    @property
    def parquet_dir(self):
        """`str`: The path to the local directory for storing parquet files.

        When defining the path, the setter will automatically create it if it
        doesn't exist. The setter will also raise a ValueError if the new path
        is not a string, or if the path exists but is not a directory.
        """
        return self.__parquet_dir

    @parquet_dir.setter
    def parquet_dir(self, path):
        if type(path) != str:
            raise ValueError("Path is not a string.")
        elif not os.path.exists(path):
            self.logger.debug(f"Creating directory '{path}'.")
            os.makedirs(path, exist_ok=True)
        elif not os.path.isdir(path):
            raise ValueError(f"'{path}' is not a directory.")

        self.__parquet_dir = path

    @property
    def processes(self) -> int:
        """`int`: The number of CPU processors to run in parallel.

        If set as `None`, the number of CPU processors available will be used
        based off `os.cpu_count()`.

        If the new value is greater than the number of CPUs available, or is
        not an integer, a ValueError will be raised.
        """
        return self.__processes

    @processes.setter
    def processes(self, processes: Optional[int]):
        if processes is None:
            self.__processes = os.cpu_count()
            self.logger.info(f"Setting pool processes to {self.processes}.")
            return

        if type(processes) != int:
            raise ValueError("Pool processes is not an integer.")
        elif processes > os.cpu_count():
            raise ValueError(f"{processes} processes is greater than the "
                             "number of CPUs available.")

        self.__processes = processes

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
        total = self.extracted + self.discarded + self.errored
        return {
            "extracted": self.extracted,
            "discarded": self.discarded,
            "errored": self.errored,
            "total": total
        }

    def __update_counters(self, counters: Dict[str, int]):
        """Add the counters from an extraction job to the total counts."""
        self.__extracted += counters.get("extracted", 0)
        self.__discarded += counters.get("discarded", 0)
        self.__errored += counters.get("errored", 0)

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

    def __load_warc_paths(self, month: int, year: int) -> List[str]:
        """Returns a list of warc files for a single month/year archive.

        Note:
            If the files for a given month/year cannot be obtained, an empty
            list is returned.

        Args:
            month (int): The month to index (between 1 and 12).
            year (int): The year to index. Must be 4 digits.

        Returns:
            List[str]: A list of warc files in the archive for records
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
            self.logger.warn(f"Failed to download paths from '{paths_url}' "
                             f"(status code {response.status_code}).")

            filenames = list()

        return filenames

    def __extract_date(self, warc_filepath: str) -> datetime:
        match = self.WARC_FILE_RE.search(warc_filepath)

        if match is None:
            self.logger.debug(f"Ignoring '{warc_filepath}'.")
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

    def __filter_warc_paths(self, filepaths: List[str]) -> List[str]:
        """Filters the list of warc filepaths to those crawled between the
        start and end dates.

        Note:
            Any filepath that doesn't match the warc filename regex is
                automatically discarded.

        Args:
            filenames (List[str]): List of warc filepaths to filter.

        Returns:
            List[str]: The filtered list of warc filepaths.
        """
        return list(filter(self.__is_within_date, filepaths))

    def retrieve_warc_paths(self, start_date: datetime,
                            end_date: datetime) -> List[str]:
        """Returns a list of warc filepaths from CC-NEWS that were crawled
        between the start and end dates.

        This done by looping through each monthly archive and extracting the
        ones that fall between the dates based on the timestamp within the
        warc filename.

        Args:
            start_date (datetime): The starting date to filter the articles
                between.
            end_date (datetime): The ending date to filter the articles
                between.

        Returns:
            List[str]: A list of warc filepaths.
        """
        self.end_date = end_date
        self.start_date = start_date

        filenames = list()

        for d in rrule(MONTHLY, self.start_date, until=self.end_date):
            self.logger.info(f"Getting WARC paths for {d.strftime('%Y-%m')}.")
            filenames.extend(self.__load_warc_paths(d.month, d.year))

        return self.__filter_warc_paths(filenames)

    @staticmethod
    def run_extraction_job(warc_url: str, patterns: List[str],
                           date_crawled: datetime, parquet_dir: str,
                           log_level: int) -> Tuple[str, Dict[str, int]]:
        """Extract all the articles from a WARC and save to a parquet file.

        Note:
            This method is designed to run as a concurrent process, so it is
            treated as a staticmethod, with all variables passed through the
            function call.

        Args:
            warc_url (str): The WARC file URL to extract aricles from.
            patterns (List[str]): The glob patterns for filtering articles
                based off the source URL.
            date_crawled (datetime): The publish date/time of the WARC file.
            parquet_dir (str): The local directory to save parquet files to.
            log_level (int): The minimum severity level the ExtractionJob logs
                should report.

        Returns:
            str: The path to the parquet file containing the articles.
            Dict[str, int]: The final counters from the extraction job.
        """
        job = ExtractionJob(warc_url, patterns, date_crawled,
                            parquet_dir, log_level)

        job.extract_warc()

        return job.filepath, job.counters

    def __on_job_success(self, result: Tuple[List[str], Dict[str, int]]):
        """The process callback for a successful extraction job.

        The callback will add the parquet file to the list of filepaths,
        updates the total counters with those from the extraction job and
        reports them.

        Args:
            result: Passed from `run_extraction_job()` as a tuple containing:
                str: The path to the parquet file containing the articles.
                Dict[str, int]: The final counters from the extraction job.
        """
        parquet_path, counters = result

        self.__update_counters(counters)
        self.report_counters()

        if counters.get("extracted", 0) > 0:
            self.parquet_files.append(parquet_path)
        else:
            self.logger.info(f"Ignoring '{parquet_path}' since it is empty.")

    def __on_job_error(self, error: Exception):
        """The process callback for a failed job. Simply logs the error."""
        self.logger.error(f"Process exited with error:\n\t{repr(error)}")

    def __submit_job(self, pool: Pool, warc_path: str, patterns: List[str]):
        """Submit an extraction job to the process pool.

        Args:
            pool (multiprocessing.pool.Pool): The process pool to submit async
                jobs to.
            warc_url (str): The WARC file URL to extract aricles from.
            patterns (List[str]): The glob patterns for filtering articles
                based off the source URL.
        """
        warc_url = urljoin(self.CC_DOMAIN, warc_path)
        date_crawled = self.__extract_date(warc_path)

        args = (warc_url, patterns, date_crawled,
                self.parquet_dir, self.log_level)

        pool.apply_async(self.run_extraction_job, args,
                         callback=self.__on_job_success,
                         error_callback=self.__on_job_error)

    def download_articles(self, patterns: List[str], start_date: datetime,
                          end_date: datetime) -> List[str]:
        """Downloads and extracts articles from CC-NEWS.

        Articles are extracted only if:
        - The source URL matches one of the URL patterns.
        - The article was crawled between the start and end dates.

        Args:
            patterns (List[str]): List of URL patterns the article must match.
            start_date (datetime): The earliest date the article must have
                been crawled.
            end_date (datetime): The latest date the article must have been
                crawled by.
        """
        self.logger.info(f"Downloading articles crawled between "
                         f"{start_date.date()} and {end_date.date()}.")

        warc_paths = self.retrieve_warc_paths(start_date, end_date)
        self.logger.info(f"Found {len(warc_paths)} WARC files to process.")

        pool = multiprocessing.Pool(processes=self.processes)

        for warc in warc_paths:
            self.__submit_job(pool, warc, patterns)

        pool.close()
        pool.join()

        return self.parquet_files
