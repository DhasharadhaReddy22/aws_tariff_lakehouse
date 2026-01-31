CREATE EXTERNAL TABLE IF NOT EXISTS `bronze`.`newsapi_articles_raw` (
  `article_url` string,
  `title` string,
  `author` string,
  `source_news` string,
  `source_id` string,
  `description` string,
  `published_at` timestamp,
  `source_api` string,
  `request_url` string,
  `received_at` timestamp,
  `ingested_at` timestamp
)
PARTITIONED BY (
  `source` string,
  `dataset` string,
  `ingestion_date` date
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'TRUE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dummy-lakehouse/bronze/news/'
TBLPROPERTIES ('classification' = 'json');

MSCK REPAIR TABLE bronze.newsapi_articles_raw; -- registers the partitions