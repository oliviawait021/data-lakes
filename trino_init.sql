CREATE SCHEMA minio.analytics
WITH (
    location = 's3a://data-lake/analytics/'
);


CREATE TABLE minio.analytics.orders (
    order_id VARCHAR,
    user_id VARCHAR,
    order_time TIMESTAMP,
    total_amount DOUBLE, 
    product_id VARCHAR,
    product_category VARCHAR,
    quantity INTEGER,
    discount_applied DOUBLE,
    payment_type VARCHAR,
    shipping_method VARCHAR,
    platform VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    external_location = 's3a://data-lake/analytics/orders/',
    format = 'PARQUET',
    partitioned_by = ARRAY['platform', 'year', 'month', 'day']
);


CREATE TABLE minio.analytics.clickstream (
    user_id VARCHAR,
    event_time TIMESTAMP,
    event_type VARCHAR,
    page VARCHAR,
    search_query VARCHAR,
    product_id VARCHAR,
    platform VARCHAR,
    year VARCHAR,
    month VARCHAR,
    day VARCHAR
)
WITH (
    external_location = 's3a://data-lake/analytics/clickstream/',
    format = 'PARQUET', 
    partitioned_by = ARRAY['platform','year','month','day']
);

CREATE TABLE minio.analytics.clickstream_no_partition (
    user_id VARCHAR,
    event_time TIMESTAMP,
    event_type VARCHAR,
    page VARCHAR,
    search_query VARCHAR,
    product_id VARCHAR
)
WITH (
    external_location = 's3a://data-lake/analytics/clickstream/',
    format = 'PARQUET'
);

CALL minio.system.sync_partition_metadata('analytics', 'orders', 'FULL');
CALL minio.system.sync_partition_metadata('analytics', 'clickstream', 'FULL');

-- Make sure you can see data from all three tables:
select * from minio.analytics.orders limit 10;
select * from minio.analytics.clickstream limit 10;
select * from minio.analytics.clickstream_no_partition limit 10;