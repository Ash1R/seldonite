import re

from collections import Counter

from pyspark.sql.types import StructType, StructField, StringType, LongType

from seldonite.spark.sparkcc import CCSparkJob
from seldonite.helpers import utils, filter, heuristics


class FetchNewsJob(CCSparkJob):
    """ News articles from from texts in Common Crawl WET files"""

    name = "FetchNewsJob"

    def process_record(self, url, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return
        if not self.is_html(record):
            self.records_non_html.add(1)
            return
        page = record.content_stream().read()

        try:
            article = utils.html_to_article(url, page)
        except Exception as e:
            self.get_logger().error("Error converting HTML to article for {}: {}",
                                    record.rec_headers['WARC-Target-URI'], e)
            self.records_parsing_failed.add(1)
            return False, None

        if not heuristics.og_type(article):
            return False, None

        if article.publish_date < self.start_date or article.publish_date > self.end_date:
            return False, None

        if self.keywords and not filter.contains_keywords(article, self.keywords):
            return False, None

        return True, { "title": article.title, "text": article.text, "url": url, "publish_date": article.publish_date }
