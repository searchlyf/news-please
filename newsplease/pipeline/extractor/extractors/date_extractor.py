import json
import re
from copy import deepcopy

from bs4 import BeautifulSoup
from dateutil.parser import parse

from .abstract_extractor import AbstractExtractor

try:
    import urllib.request as urllib2
except ImportError:
    import urllib2

# to improve performance, regex statements are compiled only once per module
re_pub_date = re.compile(
    r"([\./\-_]{0,1}(19|20)\d{2})[\./\-_]{0,1}(([0-3]{0,1}[0-9][\./\-_])|(\w{3,5}[\./\-_]))([0-3]{0,1}[0-9][\./\-]{0,1})?"
)
re_class = re.compile("pubdate|timestamp|article_date|articledate|date", re.IGNORECASE)


class DateExtractor(AbstractExtractor):
    """This class implements ArticleDateExtractor as an article extractor. ArticleDateExtractor is
    a subclass of ExtractorInterface.
    """

    def __init__(self):
        self.name = "date_extractor"

    def _pub_date(self, item):
        """Returns the pub_date of the extracted article."""

        url = item["url"]
        html = deepcopy(item["spider_response"].body)
        pub_date = None

        try:
            if html is None:
                request = urllib2.Request(url)
                # Using a browser user agent, decreases the change of sites blocking this request - just a suggestion
                # request.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)
                # Chrome/41.0.2228.0 Safari/537.36')
                html = urllib2.build_opener().open(request).read()

            html = BeautifulSoup(html, "lxml")

            pub_date = self._extract_from_json(html)
            if pub_date is None:
                pub_date = self._extract_from_meta(html)
            if pub_date is None:
                pub_date = self._extract_from_html_tag(html)
            if pub_date is None:
                pub_date = self._extract_from_url(url)
        except Exception as e:
            # print(e.message, e.args)
            pass

        return pub_date

    def parse_date_str(self, date_string):
        try:
            date = parse(date_string)
            return date.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None

    def _extract_from_url(self, url):
        """Try to extract from the article URL - simple but might work as a fallback"""

        # Regex by Newspaper3k  - https://github.com/codelucas/newspaper/blob/master/newspaper/urls.py
        m = re.search(re_pub_date, url)
        if m:
            return self.parse_date_str(m.group(0))
        return None

    def _extract_from_json(self, html):
        date = None
        try:
            script = html.find("script", type="application/ld+json")
            if script is None:
                return None

            data = json.loads(script.text)

            try:
                date = self.parse_date_str(data["datePublished"])
            except (Exception, TypeError):
                pass

            try:
                date = self.parse_date_str(data["dateCreated"])
            except (Exception, TypeError):
                pass
        except (Exception, TypeError):
            return None

        return date

    def _extract_from_meta(self, html):
        date = None
        for meta in html.findAll("meta"):
            meta_name = meta.get("name", "").lower()
            item_prop = meta.get("itemprop", "").lower()
            http_equiv = meta.get("http-equiv", "").lower()
            meta_property = meta.get("property", "").lower()

            # <meta name="DATE_PUBLISHED" content="11/24/2015 01:05AM" />
            # <meta name="cXenseParse:recs:publishtime" content="2015-11-26T14:42Z"/>
            # <meta name="article_date_original" content="Thursday, November 26, 2015,  6:42 AM" />
            # <meta name="article.created" content="2015-11-26T11:53:00.000Z" />"date_published"
            # <meta name="published-date" content="2015-11-26T11:53:00.000Z" />"cxenseparse:recs:publishtime"
            # <meta name="article.published" content="2015-11-26T11:53:00.000Z" />"article_date_original"
            # <meta name="sailthru.date" content="2015-11-25T19:56:04+0000" />"article.created"
            # <meta name="Date" content="2015-11-26" />"published-date"
            # <meta name="DC.date.issued" content="2015-11-26">"article.published"
            # <meta name="timestamp"  data-type="date" content="2015-11-25 22:40:25" />"sailthru.date"
            # <meta name="pubdate" content="2015-11-26T07:11:02Z" >
            # <meta name='publishdate' content='201511261006'/>
            if meta_name in [
                "article.created",
                "article.published",
                "article_date_original",
                "cxenseparse:recs:publishtime",
                "date",
                "date_published",
                "dc.date.issued",
                "pubdate",
                "publishdate",
                "published-date",
                "sailthru.date",
                "timestamp",
            ]:
                date = meta["content"].strip()
                break

            # <meta property="article:published_time"  content="2015-11-25" />
            if "article:published_time" == meta_property:
                date = meta["content"].strip()
                break

            # <meta property="bt:pubDate" content="2015-11-26T00:10:33+00:00">
            if "bt:pubdate" == meta_property:
                date = meta["content"].strip()
                break

            # <meta itemprop="datePublished" content="2015-11-26T11:53:00.000Z" />
            if "datepublished" == item_prop:
                date = meta["content"].strip()
                break

            # <meta itemprop="datePublished" content="2015-11-26T11:53:00.000Z" />
            if "datecreated" == item_prop:
                date = meta["content"].strip()
                break

            # <meta property="og:image" content="http://www.dailytimes.com.pk/digital
            # _images/400/2015-11-26/norway-return-number-of-asylum-seekers-to-pakistan-1448538771-7363.jpg"/>
            if "og:image" == meta_property or "image" == item_prop:
                url = meta["content"].strip()
                possible_date = self._extract_from_url(url)
                if possible_date is not None:
                    return self.parse_date_str(possible_date)

            # <meta http-equiv="data" content="10:27:15 AM Thursday, November 26, 2015">
            if "date" == http_equiv:
                date = meta["content"].strip()
                break

        if date is not None:
            return self.parse_date_str(date)

        return None

    def _extract_from_html_tag(self, html):
        # <time>
        for time in html.findAll("time"):
            datetime = time.get("datetime", "")
            if len(datetime) > 0:
                return self.parse_date_str(datetime)

            datetime = time.get("class", "")
            if len(datetime) > 0 and datetime[0].lower() == "timestamp":
                return self.parse_date_str(time.string)

        tag = html.find("span", {"itemprop": "datePublished"})
        if tag is not None:
            date_string = tag.get("content")
            if date_string is None:
                date_string = tag.text
            if date_string is not None:
                return self.parse_date_str(date_string)

        # class=
        for tag in html.find_all(["span", "p", "div"], class_=re_class):
            date_string = tag.string
            if date_string is None:
                date_string = tag.text

            date = self.parse_date_str(date_string)

            if date is not None:
                return date

        return None
