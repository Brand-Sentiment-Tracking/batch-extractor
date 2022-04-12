import logging
import json

from os import environ
from datetime import datetime, timedelta

from extractor.uploader import ArticleToParquetS3

logging.basicConfig(level=logging.INFO)

bucket_name = environ.get("S3_BUCKET_NAME")
parquet_filepath = environ.get("PARQUET_FILEPATH")
batch_upload_size = int(environ.get("BATCH_UPLOAD_SIZE"))
url_patterns = json.loads(environ.get("URL_PATTERNS"))

extractor = ArticleToParquetS3(bucket_name, parquet_filepath,
                               batch_upload_size)
        
end_date = datetime.today()
start_date = end_date - timedelta(days=1)

logging.info(f"Downloading articles crawled between "
            f"{start_date.date()} and {end_date.date()}.")

extractor.run(url_patterns, start_date, end_date)