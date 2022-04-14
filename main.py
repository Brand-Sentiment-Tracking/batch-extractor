import logging
import json

from os import environ
from datetime import date, datetime, timedelta

from extractor.uploader import ArticleToParquetS3

logging.basicConfig(level=logging.INFO)

bucket_name = environ.get("S3_BUCKET_NAME")
parquet_file = environ.get("PARQUET_FILE")
batch_size = environ.get("BATCH_SIZE")

url_patterns = environ.get("URL_PATTERNS")
start_date = environ.get("START_DATE")
end_date = environ.get("END_DATE")

batch_size = int(batch_size) \
    if batch_size is not None \
        else None

url_patterns = json.loads(url_patterns) \
    if url_patterns is not None \
        else ["*"]

if start_date is not None and end_date is not None:
    start_date = datetime.fromisoformat(start_date)
    end_date = datetime.fromisoformat(end_date)
else:
    # Use the start of the day, rather than the current time.
    end_date = datetime.fromisoformat(date.today().isoformat())
    start_date = end_date - timedelta(days=1)


uploader = ArticleToParquetS3(bucket_name, parquet_file,
                              batch_size=batch_size)

logging.info(f"Downloading articles crawled between "
             f"{start_date.date()} and {end_date.date()}.")

uploader.run(url_patterns, start_date, end_date)

logging.info("Completed Extraction job.")
uploader.report_counters()