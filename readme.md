# CC-NEWS AWS Batch Extractor

## Pre-requisites & Installation

## Usage

### `ArticleExtractor` Class

### `ArticleToParquetS3` Class

### Configuration

```docker
ENV S3_BUCKET_NAME=extracted-news-articles
ENV PARQUET_FILEPATH=v1-dev.parquet

ENV URL_PATTERNS='["*business*"]'
ENV BATCH_UPLOAD_SIZE=100
```
