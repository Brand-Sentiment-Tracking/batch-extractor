FROM python:3.6

WORKDIR /tmp/

COPY extractor.py .
COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENV WARC_DIRECTORY /tmp/warcs/
ENV ARTICLE_DIRECTORY /tmp/articles/

ENV VALID_HOST https://bbc.co.uk/*

ENV CRAWL_START_DATE 2022-02-13
ENV CRAWL_END_DATE 2022-02-14

ENV S3_BUCKET_ADDRESS something
ENV S3_PRIVATE_KEY matt_tell_me_the_api_key_pls

EXPOSE 8080
EXPOSE 443

ENTRYPOINT [ "python", "extractor.py" ]