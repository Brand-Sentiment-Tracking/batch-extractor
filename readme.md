# CC-NEWS AWS Batch Extractor

This repository holds all the code for the first stage of our Brand Sentiment Tracking system: The AWS Batch Article Extractor. When a job is submitted on AWS, this application will load all the WARC files published in CC-NEWS on the previous day (unless specified using `START_DATE` and `END_DATE`), extract all valid articles from them and push the results to an S3 Bucket (specified by `S3_BUCKET_NAME`).

## Pre-requisites & Installation

### Docker

This application has been prepared to be run on Docker, which can be built using:
```bash
docker build -t article-extractor:latest .
```

To run on AWS, make sure the image is built for `amd64`:
```bash
docker buildx build --platform=linux/amd64 -t article-extractor:latest .
```

### Running Locally

To run this package, you must have [Python 3.8](https://www.python.org/downloads/), [Java 8](https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html) and [Apache Spark 1.3.1](https://spark.apache.org/downloads.html) installed on your computer. Follow the links to install them.

Install the package requirements using pip:
```bash
pip install -r requirements.txt
```

## Usage

### `ExtractionJob` Class

`ExtractionJob` is the main class that carries out the downloading, extraction and saving of articles stored in WARC files. It is designed to be fully independent of the other classes so that it can run concurrently without any shared memory.

The main method of this class is `extract_warc()`, which given the job's initialisation will complete the whole process of downloading, extracting and saving the results from a single WARC file.

This snippet of code will download and save the first 50 articles from a WARC file published today. Note, there is no check to ensure the WARC is published on the day passed.

```python
from datetime import datetime
from extractor import ExtractionJob

job = ExtractionJob("full-url-to-warc", ["*"],
                    datetime.now(),
                    "./path/to/parquet/files")


job.extract_warc(limit=50)
```

### `ArticleExtractor` Class

The `ArticleExtractor` class finds all the relevant WARC files to process and creates multiple `ExtractionJob` using a process pool to speed up extraction. The main method for this class is `download_articles()`, which will extract all the articles that match a list of URL patterns (based off the `glob` library) and the start and end dates passed to the function.


### `ArticleToParquetS3` Class

The `ArticleToParquetS3` class handles uploading all the parquet files saved in a local directory to S3, using Apache Spark. The main method of this class is `run()`, which combines all the classes above, as well as a final uploading step to S3. 

### Configuration

Configuration for the job in `main.py` is passed as environment variables:
- `S3_BUCKET_NAME`: The name of the bucket to push the parquet files to in S3.
- `URL_PATTERNS`: A list of glob patterns to match the article's URL against when filtering.
- `START_DATE`: The start date in ISO format to filter the WARC files by.
- `END_DATE`: The end date (not inclusive) in ISO format to filter the WARC files by.
- `MAX_RECORDS`: The maximum number of articles to save to each parquet file.

If start date or end date is not specified, the extraction will run for the previous day. If no URL patterns are passed, all URLs will be accepted.

### Testing

To run unit tests locally, use:
```bash
pytest extractor
```

To run in Docker, use:
```bash
docker run --entrypoint=./test.sh article-extractor:latest
```

Make sure you pass the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to test uploading and downloading to AWS.
