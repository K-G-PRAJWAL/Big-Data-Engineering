YouTube Data Anlaysis pipeline to extract data from CSV/JSON([Kaggle Source](https://www.kaggle.com/datasets/datasnaek/youtube-new)) into BI Anlaytics.

Architecture:

![](https://github.com/K-G-PRAJWAL/Big-Data-Engineering/blob/main/Projects/YouTube-Data-Pipeline/arch.png)

Commands to copy data from local to AWS S3 via AWSCLI:

aws s3 cp . s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"

aws s3 cp  .\CAvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=ca/
aws s3 cp  .\DEvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=de/
aws s3 cp  .\FRvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=fr/
aws s3 cp  .\GBvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=gb/
aws s3 cp  .\INvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=in/
aws s3 cp  .\JPvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=jp/
aws s3 cp  .\KRvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=kr/
aws s3 cp  .\MXvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=mx/
aws s3 cp  .\RUvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=ru/
aws s3 cp  .\USvideos.csv  s3://youtube-de-pipeline-raw-dev/youtube/raw_statistics/region=us/


