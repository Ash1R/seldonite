import datetime
import gzip
import re

import botocore
import boto3
import requests

from seldonite.model import Article

def link_to_article(link):
    article = Article(link)
    article.download()
    article.parse()

    return article

def html_to_article(url, html, title=None):
    article = Article(url)
    article.download(input_html=html)
    article.parse()

    if title is not None:
        article.set_title(title)

    return article

def dict_to_article(dict):
    article = Article(dict['url'])
    article.set_title(dict['title'])
    article.set_text(dict['text'])
    article.publish_date = dict['publish_date']
    return article

def get_crawl_listing(crawl, data_type="wet"):
    url = f"https://commoncrawl.s3.amazonaws.com/crawl-data/{crawl}/{data_type}.paths.gz"
    res = requests.get(url)
    txt_listing = gzip.decompress(res.content).decode("utf-8")
    listing = txt_listing.splitlines()
    return ['s3://commoncrawl/' + entry for entry in listing]

def get_news_crawl_listing(start_date=None, end_date=None):
    no_sign_request = botocore.client.Config(
        signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)
    s3_paginator = s3client.get_paginator('list_objects_v2')

    def keys(bucket_name, prefix='/', delimiter='/', start_after=''):
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        start_after = (start_after or prefix) if prefix.endswith(delimiter) else start_after
        for page in s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix, StartAfter=start_after):
            for content in page.get('Contents', ()):
                yield content['Key']

    warc_paths = []
    if not start_date and not end_date:
        for key in keys('commoncrawl', prefix='/crawl-data/CC-NEWS/'):
            warc_paths.append(key)
        return warc_paths

    if not end_date:
        end_date = datetime.date.today()
    elif not start_date:
        start_date = datetime.date.min
    
    delta = end_date - start_date

    # pad ending files to account for time that pages spend in sitemap and rss feed
    # normally roughly 30 days
    sitemap_pad = 30

    days = []
    for i in range(delta.days + 1 + sitemap_pad):
        days.append(start_date + datetime.timedelta(days=i))

    for day in days:
        date_path = day.strftime('%Y/%m/CC-NEWS-%Y%m%d')
        for key in keys('commoncrawl', prefix=f'/crawl-data/CC-NEWS/{date_path}'):
                warc_paths.append(key)

    return warc_paths

def get_all_cc_crawls():
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()
    return [crawl['id'] for crawl in crawls]

def most_recent_cc_crawl():
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()
    return crawls[0]['id']

def get_cc_crawls_since(date):
    url = 'https://index.commoncrawl.org/collinfo.json'
    res = requests.get(url)
    crawls = res.json()

    year_regex = r'[0-9]{4}'
    month_regex = r'January|February|March|April|May|June|July|August|September|October|November|December'
    crawl_ids = []
    for crawl in crawls:
        crawl_years = [int(year) for year in re.findall(year_regex, crawl['name'])]
        crawl_year = min(crawl_years)
        if crawl_year > date.year:
            crawl_ids.append(crawl['id'])
        elif crawl_year == date.year:
            crawl_month_match = re.search(month_regex, crawl['name'])
            if not crawl_month_match:
                continue

            crawl_month = crawl_month_match.group()
            crawl_month_date = datetime.datetime.strptime(crawl_month, '%B')
            crawl_month_num = crawl_month_date.month
            if crawl_month_num > date.month:
                crawl_ids.append(crawl['id'])

    return crawl_ids

def construct_query(sites, limit, crawls=None, type=None):
    #TODO automatically get most recent crawl
    query = "SELECT url, warc_filename, warc_record_offset, warc_record_length, content_charset FROM ccindex WHERE subset = 'warc'"

    if crawls:
        # 
        if len(crawls) == 1:
            query += f" AND crawl = '{crawls[0]}'"
        else:
            crawl_list = ', '.join([f"'{crawl}'" for crawl in crawls])
            query += f" AND crawl IN ({crawl_list})"

    # site restrict
    if not all("." in domain for domain in sites):
        raise ValueError("Sites should be the full registered domain, i.e. cbc.ca instead of just cbc")

    if sites:
        site_list = ', '.join([f"'{site}'" for site in sites])
        query += f" AND url_host_registered_domain IN ({site_list})"

    # set limit to sites if needed
    if limit:
        query += f" LIMIT {str(limit)}"

    return query
