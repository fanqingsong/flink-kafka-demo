CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.ab_events
(
    event_id UUID,
    event_name LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    user_id String,
    session_id String,
    experiment_id UUID,
    variant_id UUID,
    properties String,
    attributed UInt8,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_time)
ORDER BY (experiment_id, variant_id, event_time, event_id);

CREATE TABLE IF NOT EXISTS analytics.ab_events_kafka
(
    event_id UUID,
    event_name String,
    event_time DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    user_id String,
    session_id String,
    experiment_id UUID,
    variant_id UUID,
    properties String,
    attributed UInt8,
    version UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ab.events.valid',
    kafka_group_name = 'clickhouse-ab-events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.ab_events_mv
TO analytics.ab_events
AS SELECT * FROM analytics.ab_events_kafka;

CREATE TABLE IF NOT EXISTS analytics.ab_metrics_1m
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    experiment_id UUID,
    variant_id UUID,
    exposures UInt64,
    clicks UInt64,
    purchases UInt64,
    revenue Decimal(18, 2),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(window_start)
ORDER BY (experiment_id, variant_id, window_start);

CREATE TABLE IF NOT EXISTS analytics.ab_metrics_1m_kafka
(
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    experiment_id UUID,
    variant_id UUID,
    exposures UInt64,
    clicks UInt64,
    purchases UInt64,
    revenue Decimal(18, 2),
    version UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ab.metrics.1m',
    kafka_group_name = 'clickhouse-ab-metrics',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.ab_metrics_1m_mv
TO analytics.ab_metrics_1m
AS SELECT * FROM analytics.ab_metrics_1m_kafka;
