#!/usr/bin/env python
"""
This scripts downloads WARC files from commoncrawl.org's news crawl and extracts articles from these files. You can
define filter criteria that need to be met (see YOUR CONFIG section), otherwise an article is discarded. Currently, the
script stores the extracted articles in JSON files, but this behaviour can be adapted to your needs in the method
on_valid_article_extracted. To speed up the crawling and extraction process, the script supports multiprocessing. You can
control the number of processes with the parameter process_num.

You can also crawl and extract articles programmatically, i.e., from within your own code, by using the class
CommonCrawlCrawler provided in newsplease.crawler.commoncrawl_crawler.py

In case the script crashes and contains a log message in the beginning that states that only 1 file on AWS storage
was found, make sure that awscli was correctly installed. You can check that by executing aws --version from a terminal.
If aws is not installed, you can (on Ubuntu) also install it using sudo apt-get install awscli.

This script uses relative imports to ensure that the latest, local version of news-please is used, instead of the one
that might have been installed with pip. Hence, you must run this script following this workflow.
git clone https://github.com/fhamborg/news-please.git
cd news-please
python3 -m newsplease.commoncrawl
"""
import argparse
import logging
from crawler.commoncrawl import CommonCrawler


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="fetch commoncrawl.org")
    parser.add_argument("--valid_hosts", nargs="*", help="None means any host")
    parser.add_argument(
        "--start_date", default="2019-08-01", help="None means any date"
    )
    parser.add_argument("--end_date")
    parser.add_argument("--data_dir", default="/mnt/d/data", help="warc data dir")
    parser.add_argument(
        "--process_num", default=1, help="number of extraction processes"
    )

    args = parser.parse_args()
    cc = CommonCrawler(
        valid_hosts=args.valid_hosts,
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=args.data_dir,
        process_num=args.process_num,
    )
    cc.crawl()
