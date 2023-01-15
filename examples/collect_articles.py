import os

from seldonite import sources, collect, run, graphs
import datetime
aws_access_key = os.environ['AWS_ACCESS_KEY']
aws_secret_key = os.environ['AWS_SECRET_KEY']
print(aws_access_key)
source = sources.news.CommonCrawl(aws_access_key, aws_secret_key)

collector = collect.Collector(source) \
    .from_urls(['bbc.com']) \
    .by_keywords(['afghanistan']) \
    .limit_num_articles(10) \
    .in_date_range(datetime.date(2021, 8, 25), datetime.date(2022, 8, 26))
print("collected")
runner = run.Runner(collector) 
df = runner.to_pandas()

df.to_csv("deeznuts.csv")
