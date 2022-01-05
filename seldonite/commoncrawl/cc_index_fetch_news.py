
import pyspark.sql as psql

from seldonite import filters
from seldonite.commoncrawl.sparkcc import CCIndexWarcSparkJob
from seldonite.commoncrawl.fetch_news import FetchNewsJob
from seldonite.helpers import heuristics, utils


class CCIndexFetchNewsJob(CCIndexWarcSparkJob, FetchNewsJob):
    """ News articles from WARC records matching a SQL query
        on the columnar URL index """

    name = "CCIndexFetchNewsJob"

    records_parsing_failed = None
    records_non_html = None
        
    def run(self, spark_manager, keywords=[], start_date=None, end_date=None, **kwargs):
        self.set_constraints(keywords, start_date, end_date)
        return super().run(spark_manager, **kwargs)

    def set_query_options(self, sites=[], crawls=[], lang=None, limit=None, path_black_list=[]):
        self.query = utils.construct_query(sites, limit, crawls=crawls, lang=lang, path_black_list=path_black_list)

    def init_accumulators(self, spark_manager):
        super().init_accumulators(spark_manager)

        sc = spark_manager.get_spark_context()
        self.records_parsing_failed = sc.accumulator(0)
        self.records_non_html = sc.accumulator(0)

    def log_aggregators(self, spark_manager):
        super().log_aggregators(spark_manager)

        self.log_aggregator(spark_manager, self.records_parsing_failed,
                            'records failed to parse = {}')
        self.log_aggregator(spark_manager, self.records_non_html,
                            'records not HTML = {}')


    def process_record(self, record):
        if record.rec_type != 'response':
            # skip over WARC request or metadata records
            return None
        if not self.is_html(record):
            return None

        url = record.rec_headers.get_header('WARC-Target-URI')

        return self._process_record(url, record)
