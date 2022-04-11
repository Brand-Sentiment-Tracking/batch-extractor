import os
import logging

from datetime import datetime, date, timedelta
from newsplease.crawler import commoncrawl_crawler as cc

logging.basicConfig(level=logging.DEBUG)


def empty_callback(*args):
    pass


today = date.today()
today_date = datetime(today.year, today.month, today.day)

end_date = today_date - timedelta(days=1)
start_date = end_date - timedelta(days=1)

os.makedirs("./warcs/", exist_ok=True)

cc.crawl_from_commoncrawl(
    valid_hosts=["bbc.co.uk"],
    warc_files_start_date=start_date,
    warc_files_end_date=end_date,
    callback_on_article_extracted=empty_callback,
    callback_on_warc_completed=empty_callback,
    continue_after_error=True,
    local_download_dir_warc="./warcs/",
    number_of_extraction_processes=1,
    log_level=logging.INFO)
