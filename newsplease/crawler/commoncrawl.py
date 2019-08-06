"""
commoncrawl.org
Provides functionality to crawl and extract news articles from a single WARC file from
commoncrawl.org. Filter criteria, such as publish date and host list, can be defined.
Currently, the WARC file will be downloaded to the path WORKINGDIR/cc_warc, if
not otherwise specified.
"""
import json
import logging
import os
import subprocess
import sys
import time
import urllib

from functools import partial
from multiprocessing import Pool

from six.moves import urllib
from warcio.archiveiterator import ArchiveIterator
from newsplease import NewsPlease

logger = logging.getLogger(__name__)


class CommonCrawler:
    def __init__(self, valid_hosts, start_date, end_date, data_dir, process_num):
        self.valid_hosts = valid_hosts.split() if valid_hosts else []
        self.start_date = start_date if start_date else "2019-01-01"
        self.end_date = end_date if end_date else "2099-01-01"
        self.data_dir = data_dir
        self.process_num = process_num

        # download dir for warc files
        self.warc_dir = os.path.join(data_dir, "cc_warc/")
        # download dir for articles
        self.article_dir = os.path.join(data_dir, "cc_articles/")
        # log file of fully extracted WARC files
        self.extracted_warc_file = os.path.join(data_dir, "fullyextractedwarcs.list")
        os.makedirs(self.article_dir, exist_ok=True)
        os.makedirs(self.warc_dir, exist_ok=True)

        self.base_url = "https://commoncrawl.s3.amazonaws.com/"
        self.downloaded_urls = []

    def _get_filepath(self, article):
        path = os.path.join(self.article_dir, article.source_domain)
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, article.filename + ".json")

    def process_article(self, article):
        """
        This function will be invoked for each article that was extracted successfully
        from the archived data and that satisfies the filter criteria.
        :param article:
        :return:
        """
        with open(self._get_filepath(article), "w") as out:
            json.dump(article.get_serializable_dict(), out, indent=4, ensure_ascii=False)

    def dump_downloaded_urls(self):
        """
        Saves the URL warc_url in the log file for fully extracted WARC URLs
        :param warc_url:
        :return:
        """
        with open(self.extracted_warc_file, "a") as f:
            for url in self.downloaded_urls:
                f.write(url + "\n")

    def is_wanted_record(self, warc_record):
        """
        Returns true if a record passes all tests: hosts, publish date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """
        # filter by host. ex: 'www.cctv.com'
        if self.valid_hosts:
            url = warc_record.rec_headers.get_header("WARC-Target-URI")
            domain = urllib.parse.urlparse(url).hostname
            if domain not in self.valid_hosts:
                return False
        # filter by date. ex: '2019-07-20T05:40:25Z'
        d = warc_record.rec_headers.get_header("warc-date")
        return self.start_date <= d and d < self.end_date

    def get_remote_index(self):
        """
        Gets the index of news crawl files from commoncrawl.org and returns an array of names
        :return:
        """
        # cleanup
        subprocess.getoutput("rm ~/tmp/tmpaws.txt")
        # get the remote info
        cmd = (
            "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request > ~/tmp/tmpaws.txt && "
            "awk '{ print $4 }' ~/tmp/tmpaws.txt | grep 20190801"
        )
        logger.info("executing: %s", cmd)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            return []
        logger.info(f" warc list:\n{output}")
        return output.splitlines()

    def download_warc_file(self, url):
        """
        Download and save a file locally.
        :param url: Where to download from
        :return: File path name of the downloaded file
        """
        filename = os.path.join(self.warc_dir, urllib.parse.quote_plus(url))
        # if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
        # again. Note that there is no check whether the file has been downloaded completely or is valid!
        if os.path.isfile(filename):
            logger.info("found local file %s: ", filename)
            return filename
        logger.info("downloading %s (local: %s)", url, filename)
        urllib.request.urlretrieve(url, filename)
        logger.info("download completed, local file: %s", filename)
        return filename

    def process_warc_file(self, path_name):
        """
        Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Afterwards, each article is checked against the filter criteria and if all are passed, the function
        save_article is invoked with the article object.
        :param path_name:
        :return:
        """
        total = 0
        passed = 0
        discarded = 0
        error = 0
        start_time = time.time()

        with open(path_name, "rb") as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type != "response":
                    logger.warning("WARC-Type: is not response")
                    continue
                total += 1
                # if the article passes filter tests, we notify the user
                if self.is_wanted_record(record):
                    passed += 1
                    article = NewsPlease.from_warc(record)
                    self.process_article(article)
                else:
                    discarded += 1
                    logger.debug(
                        "article discard: %s)",
                        record.rec_headers.get_header("WARC-Target-URI"),
                    )
                if total % 100 == 0:
                    logger.info(
                        "pass = %i, discard = %i, error = %i, total = %i",
                        passed,
                        discarded,
                        error,
                        total,
                    )
        secs_per_article = (time.time() - start_time) / total
        logger.info(f"extracting WARC {secs_per_article} s/article")
        self.downloaded_urls.append(self.url)

    def get_extracted_warc_urls(self):
        if not os.path.isfile(self.extracted_warc_file):
            return []
        with open(extracted_warc_file, "r") as log_file:
            return [x.strip() for x in log_file.readlines()]

    def run(self, url):
        """
        get an up-to-date list of WARC files, for each of them:
        download and extract articles.
        Each article is checked against a filter.
        save_article will be invoked after the extraction of the article.
        :return:
        """
        file_name = self.download_warc_file(url)
        self.process_warc_file(file_name)

    def crawl(self):
        warc_names = self.get_remote_index()
        logger.info("found %i files at commoncrawl.org", len(warc_names))
        extracted_urls = self.get_extracted_warc_urls()
        urls = []
        for name in warc_names:
            url = self.base_url + name
            # check if the current WARC has already been fully extracted
            # (assuming that the filter criteria have not been changed!)
            if url in extracted_urls:
                logger.info("skipping WARC because fully extracted: %s" % url)
            else:
                urls.append(url)
        logger.info(f"{self.process_num} processes for {len(urls)} urls")
        with Pool(processes=self.process_num) as pool:
            pool.map(self.run, urls)
