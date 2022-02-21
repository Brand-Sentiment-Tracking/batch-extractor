FROM python:3.6

WORKDIR /tmp/

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY extractor.py .

ENV WARC_DIRECTORY      '/tmp/warcs/'
ENV ARTICLE_DIRECTORY   '/tmp/articles/'

ENV VALID_HOSTS         '["bbc.co.uk"]'

ENV CRAWL_START_DATE    '2022-02-13'
ENV CRAWL_END_DATE      '2022-02-14'

ENV S3_BUCKET_ADDRESS   'bucket_name'
ENV AWS_ACCESS_KEY      'access_key'

ENV ENVIRONMENT_TYPE    'dev'

EXPOSE 8080
EXPOSE 443

ENTRYPOINT [ "python", "extractor.py" ]